package org.infinispan.scattered.statetransfer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ControlledTransport;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TopologyChangeListener;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.transport.DelayedViewJGroupsTransport;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.BlockingLocalTopologyManager.BlockedTopology;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scattered.statetransfer.CoordinatorStopTest")
@CleanupAfterMethod
public class CoordinatorStopTest extends MultipleCacheManagersTest {
   private CountDownLatch viewLatch;
   private ControlledConsistentHashFactory.Scattered chf;

   @Override
   public Object[] factory() {
      return new Object[] {
            new CoordinatorStopTest().biasAcquisition(BiasAcquisition.NEVER),
            new CoordinatorStopTest().biasAcquisition(BiasAcquisition.ON_WRITE)
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      chf = new ControlledConsistentHashFactory.Scattered(new int[] {0, 1, 2});
      cb.clustering().cacheMode(CacheMode.SCATTERED_SYNC).hash().numSegments(3).consistentHashFactory(chf);
      if (biasAcquisition != null) {
         cb.clustering().biasAcquisition(biasAcquisition);
      }

      addClusterEnabledCacheManager(cb);
      // If the updated topologies from old coord come when it's no longer in the view these are ignored.
      // Therefore we have to delay the view.
      viewLatch = new CountDownLatch(1);
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.transport().transport(new DelayedViewJGroupsTransport(viewLatch));
      addClusterEnabledCacheManager(gcb, cb);
      // we need distinct transport instances on manager(1) and (2)
      gcb.transport().transport(new DelayedViewJGroupsTransport(viewLatch));
      addClusterEnabledCacheManager(gcb, cb);

      assertTrue(cache(0).getCacheManager().isCoordinator());
      // start other caches
      cache(1);
      cache(2);
      waitForClusterToForm();
   }

   // Reproducer for ISPN-9128
   public void testCoordinatorLeaves() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      String cacheName = cache(1).getName();
      MagicKey key = new MagicKey(cache(0));
      cache(1).put(key, "value");

      int stableTopologyId = cache(1).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();

      BlockingLocalTopologyManager ltm2 = BlockingLocalTopologyManager.replaceTopologyManager(manager(2), cacheName);
      ControlledTransport transport0 = ControlledTransport.replace(cache(0));
      ControlledTransport transport1 = ControlledTransport.replace(cache(1));
      // Block sending REBALANCE_START until the CH_UPDATE is delivered to make the test deterministic
      transport0.blockBefore(cmd -> {
         if (cmd instanceof CacheTopologyControlCommand) {
            CacheTopologyControlCommand command = (CacheTopologyControlCommand) cmd;
            if (command.getCacheName().equals(cacheName) &&
                  command.getTopologyId() == stableTopologyId + 2 &&
                  command.getType() == CacheTopologyControlCommand.Type.REBALANCE_START) {
               return true;
            }
         }
         return false;
      });
      // Also block rebalance initiated by the new coord until we test with topology + 3
      transport1.blockBefore(cmd -> {
         if (cmd instanceof CacheTopologyControlCommand) {
            CacheTopologyControlCommand command = (CacheTopologyControlCommand) cmd;
            if (command.getCacheName().equals(cacheName) &&
                  command.getTopologyId() == stableTopologyId + 4 &&
                  command.getType() == CacheTopologyControlCommand.Type.REBALANCE_START) {
               return true;
            }
         }
         return false;
      });

      ControlledRpcManager rpcManager2 = ControlledRpcManager.replaceRpcManager(cache(2));
      // Ignore push transfer of segment 2
      // Ignore the remote get which does not happen without the fix
      rpcManager2.excludeCommands(StateResponseCommand.class, ClusteredGetCommand.class);

      // segment 0 will be moved to cache(2). Since we've lost coord cache(1) now -> 0 and cache(2) -> 1
      chf.setOwnerIndexes(new int[][] { {1}, {0}, {1} });

      log.infof("Stopping coordinator %s, last stable topology is %d", manager(0), stableTopologyId);
      Future<Void> stopFuture = fork(() -> manager(0).stop());

      // topology + 1 is the one that just omits the leaving node
      BlockedTopology t1 = ltm2.expectTopologyUpdate(CacheTopology.Phase.NO_REBALANCE, stableTopologyId + 1);
      if (t1.getCacheTopology().getTopologyId() == stableTopologyId + 1)
      assertEquals(CacheTopology.Phase.NO_REBALANCE, t1.getPhase());
      assertEquals(2, t1.getCacheTopology().getActualMembers().size());
      assertEquals(null, t1.getCacheTopology().getPendingCH());
      assertOwners(t1, true, 0);
      assertOwners(t1, true, 1, address(1));
      assertOwners(t1, true, 2, address(2));
      t1.unblock();

      transport0.stopBlocking();
      stopFuture.get(10, TimeUnit.SECONDS);
      // It is not guaranteed that all members got new view when stop() finishes - when the coord is leaving
      // the members ack the view before installing it.
      // We are delaying view 3 until topology + 2 is installed on cache(1) - therefore at this point manager(1)
      // is not the coordinator yet, and we have 3 members in view

      // topology + 2 has TRANSITORY phase and all segments have an owner in pendingCH
      BlockedTopology t2 = ltm2.expectTopologyUpdate(CacheTopology.Phase.TRANSITORY, stableTopologyId + 2);
      assertEquals(CacheTopology.Phase.TRANSITORY, t2.getPhase());
      assertEquals(2, t2.getCacheTopology().getActualMembers().size());
      assertNotNull(t2.getCacheTopology().getPendingCH());
      assertOwners(t2, false, 0, address(2));
      assertOwners(t2, false, 1, address(1));
      assertOwners(t2, false, 2, address(2));
      t2.unblock();

      // Let the rebalance begin
      rpcManager2.expectCommand(StateRequestCommand.class,
            request -> assertEquals(StateRequestCommand.Type.CONFIRM_REVOKED_SEGMENTS, request.getType()))
            .send().receiveAll();
      // Allow both nodes to receive the view. If we did not block (1), too, topology + 2 could be ignored
      // on cache(1) and the CONFIRM_REVOKED_SEGMENTS would get blocked until topology + 3 arrives - and this
      // does not happen before the test times out.
      viewLatch.countDown();

      ControlledRpcManager.BlockedRequest keyTransferRequest = rpcManager2.expectCommand(StateRequestCommand.class,
            request -> assertEquals(StateRequestCommand.Type.START_KEYS_TRANSFER, request.getType()));

      // topology + 3 should have null pendingCH
      // Before the fix the topology would recover transitory topologies from above and base current CH on them
      BlockedTopology t3 = ltm2.expectTopologyUpdate(CacheTopology.Phase.NO_REBALANCE, stableTopologyId + 3);
      assertEquals(2, t3.getCacheTopology().getActualMembers().size());
      assertEquals(null, t3.getCacheTopology().getPendingCH());

      TopologyChangeListener topologyChangeListener = TopologyChangeListener.install(cache(2));
      ltm2.stopBlocking();
      t3.unblock();

      // Cancel command is sent only with the fix in
      if (t3.getCacheTopology().getCurrentCH().locatePrimaryOwnerForSegment(0) == null) {
         ControlledRpcManager.BlockedRequest cancelStateTransfer = rpcManager2.expectCommand(StateRequestCommand.class,
               request -> assertEquals(StateRequestCommand.Type.CANCEL_STATE_TRANSFER, request.getType()));
         cancelStateTransfer.send();
      }

      // Wait until topology + 3 is installed
      topologyChangeListener.await(10, TimeUnit.SECONDS);

      // unblock outdated keys transfer
      keyTransferRequest.send().receiveAll();

      CyclicBarrier oteBarrier = new CyclicBarrier(2);
      BlockingInterceptor oteInterceptor = new BlockingInterceptor(oteBarrier, GetKeyValueCommand.class, true, true);
      cache(2).getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(oteInterceptor, StateTransferInterceptor.class);

      // The get is supposed to retry as the primary owner is null in topology + 3
      Future<Object> future = fork(() -> cache(2).get(key));

      // This barrier will wait until the command returns, in any way. Without the fix it should just return null,
      // with the fix it should throw OTE and we'll be waiting for the next topology - that's why we have to unblock it.
      oteBarrier.await(10, TimeUnit.SECONDS);
      oteInterceptor.suspend(true);

      rpcManager2.stopBlocking();
      transport1.stopBlocking();
      oteBarrier.await(10, TimeUnit.SECONDS);

      assertEquals("value", future.get());
   }

   private void assertOwners(BlockedTopology t, boolean current, int segmentId, Address... address) {
      ConsistentHash ch = current ? t.getCacheTopology().getCurrentCH() : t.getCacheTopology().getPendingCH();
      assertEquals("Topology: " + t.getCacheTopology(), Arrays.asList(address), ch.locateOwnersForSegment(segmentId));
   }
}
