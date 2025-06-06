package org.infinispan.distribution.rehash;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.op.TestWriteOperation;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Tests that a conditional write is retried properly if the write is unsuccessful on the primary owner
 * because it became a non-owner and doesn't have the entry any more.
 *
 * See https://issues.jboss.org/browse/ISPN-3830
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPrimaryOwnerBecomingNonOwnerTest")
@CleanupAfterMethod
public class NonTxPrimaryOwnerBecomingNonOwnerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();
      createCluster(DistributionRehashSCI.INSTANCE, c, 2);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.clustering().hash().numSegments(1);
      c.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(new CustomConsistentHashFactory());
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   public void testPrimaryOwnerChangingDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE);
   }

   public void testPrimaryOwnerChangingDuringPutIfAbsent() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT);
   }

   public void testPrimaryOwnerChangingDuringReplace() throws Exception {
      doTest(TestWriteOperation.REPLACE);
   }

   public void testPrimaryOwnerChangingDuringReplaceExact() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT);
   }

   public void testPrimaryOwnerChangingDuringRemove() throws Exception {
      doTest(TestWriteOperation.REMOVE);
   }

   public void testPrimaryOwnerChangingDuringRemoveExact() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT);
   }

   private void doTest(final TestWriteOperation op) throws Exception {
      final String key = "testkey";
      final String cacheName = manager(0).getCacheManagerConfiguration().defaultCacheName().get();
      if (op.getPreviousValue() != null) {
         cache(0, cacheName).put(key, op.getPreviousValue());
      }

      CheckPoint checkPoint = new CheckPoint();
      LocalTopologyManager ltm0 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      int preJoinTopologyId = ltm0.getCacheTopology(cacheName).getTopologyId();
      int joinTopologyId = preJoinTopologyId + 1;
      int stateReceivedTopologyId = joinTopologyId + 1;

      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      addBlockingLocalTopologyManager(manager(0), checkPoint, joinTopologyId, stateReceivedTopologyId);

      final AdvancedCache<Object, Object> cache1 = advancedCache(1);
      addBlockingLocalTopologyManager(manager(1), checkPoint, joinTopologyId, stateReceivedTopologyId);

      // Add a new member and block the rebalance before the final topology is installed
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(DistributionRehashSCI.INSTANCE, c);
      addBlockingLocalTopologyManager(manager(2), checkPoint, joinTopologyId, stateReceivedTopologyId);

      log.tracef("Starting the cache on the joiner");
      final AdvancedCache<Object,Object> cache2 = advancedCache(2);

      checkPoint.trigger("allow_topology_" + joinTopologyId + "_on_" + address(0));
      checkPoint.trigger("allow_topology_" + joinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + joinTopologyId + "_on_" + address(2));

      // Wait for the write CH to contain the joiner everywhere
      Stream.of(cache0, cache1, cache2).forEach(cache ->
            eventuallyEquals(3, () -> cache.getRpcManager().getMembers().size()));

      CacheTopology duringJoinTopology = ltm0.getCacheTopology(cacheName);
      assertEquals(CacheTopology.Phase.READ_OLD_WRITE_ALL, duringJoinTopology.getPhase());
      assertEquals(joinTopologyId, duringJoinTopology.getTopologyId());
      assertNotNull(duringJoinTopology.getPendingCH());
      int keySegment = TestingUtil.getSegmentForKey(key, cache0);
      log.tracef("Rebalance started. Found key %s with current owners %s and pending owners %s", key,
            duringJoinTopology.getCurrentCH().locateOwnersForSegment(keySegment), duringJoinTopology.getPendingCH().locateOwnersForSegment(keySegment));

      // Every operation command will be blocked before reaching the distribution interceptor on cache0 (the originator)
      CyclicBarrier beforeCache0Barrier = new CyclicBarrier(2);
      BlockingInterceptor<?> blockingInterceptor0 = new BlockingInterceptor<>(beforeCache0Barrier,
            op.getCommandClass(), false, true);
      extractInterceptorChain(cache0).addInterceptorBefore(blockingInterceptor0, EntryWrappingInterceptor.class);

      // Write from cache0 with cache0 as primary owner, cache2 will become the primary owner for the retry
      Future<Object> future = fork(() -> op.perform(cache0, key));

      // Block the write command on cache0
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);

      // Allow the topology update to proceed on cache0
      checkPoint.trigger("allow_topology_" + stateReceivedTopologyId + "_on_" + address(0));
      eventuallyEquals(stateReceivedTopologyId,
            () -> cache0.getDistributionManager().getCacheTopology().getTopologyId());
      assertEquals(CacheTopology.Phase.READ_ALL_WRITE_ALL, cache0.getDistributionManager().getCacheTopology().getPhase());

      // Allow the command to proceed
      log.tracef("Unblocking the write command on node " + address(1));
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);

      // Wait for the retry after the OutdatedTopologyException
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);
      // Do not block during (possible) further retries, and allow it to proceed
      blockingInterceptor0.suspend(true);
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);

      // Allow the topology update to proceed on the other caches
      checkPoint.trigger("allow_topology_" + stateReceivedTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + stateReceivedTopologyId + "_on_" + address(2));

      // Wait for the topology to change everywhere
      TestingUtil.waitForNoRebalance(cache0, cache1, cache2);

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      // TODO ISPN-7590: Return values are not reliable, if the command is retried after being applied to both backup
      // owners the retry will provide incorrect return value
//      assertEquals(op.getReturnValue(), result);
      log.tracef("Write operation is done");

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));
      assertEquals(op.getValue(), cache2.get(key));

      // Check that there are no leaked locks
      assertFalse(cache0.getAdvancedCache().getLockManager().isLocked(key));
      assertFalse(cache1.getAdvancedCache().getLockManager().isLocked(key));
      assertFalse(cache2.getAdvancedCache().getLockManager().isLocked(key));
   }

   @ProtoName("PrimaryOwnerCustomConsistentHashFactory")
   public static class CustomConsistentHashFactory extends BaseControlledConsistentHashFactory.Default {
      CustomConsistentHashFactory() {
         super(1);
      }

      @Override
      protected int[][] assignOwners(int numSegments, List<Address> members) {
         switch (members.size()) {
            case 1:
               return new int[][]{{0}};
            case 2:
               return new int[][]{{0, 1}};
            default:
               return new int[][]{{members.size() - 1, 0}};
         }
      }
   }

   private void addBlockingLocalTopologyManager(final EmbeddedCacheManager manager, final CheckPoint checkPoint,
         final Integer... blockedTopologyIds) {
      LocalTopologyManager component = TestingUtil.extractGlobalComponent(manager, LocalTopologyManager.class);
      LocalTopologyManager spyLtm = spy(component);
      doAnswer(invocation -> {
         CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
         // Ignore the first topology update on the joiner, which is with the topology before the join
         if (Arrays.asList(blockedTopologyIds).contains(topology.getTopologyId())) {
            checkPoint.trigger("pre_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress());
            checkPoint.awaitStrict("allow_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress(),
                  10, TimeUnit.SECONDS);
         }
         return invocation.callRealMethod();
      }).when(spyLtm).handleTopologyUpdate(eq(TestingUtil.getDefaultCacheName(manager)), any(CacheTopology.class),
                                              any(AvailabilityMode.class), anyInt(), any(Address.class));
      TestingUtil.replaceComponent(manager, LocalTopologyManager.class, spyLtm, true);
   }
}
