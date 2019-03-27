package org.infinispan.distribution.rehash;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer a backup owner of a key becomes the primary owner
 * modified key while a write operation is in progress.
 * See https://issues.jboss.org/browse/ISPN-3357
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxBackupOwnerBecomingPrimaryOwnerTest")
@CleanupAfterMethod
public class NonTxBackupOwnerBecomingPrimaryOwnerTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = BasicCacheContainer.DEFAULT_CACHE_NAME;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.clustering().hash().numSegments(1).consistentHashFactory(new CustomConsistentHashFactory());
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   public void testPrimaryOwnerChangingDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE);
   }

   public void testPrimaryOwnerChangingDuringPutOverwrite() throws Exception {
      doTest(TestWriteOperation.PUT_OVERWRITE);
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

   protected void doTest(final TestWriteOperation op) throws Exception {
      final String key = "testkey";
      if (op.getPreviousValue() != null) {
         cache(0, CACHE_NAME).put(key, op.getPreviousValue());
      }

      CheckPoint checkPoint = new CheckPoint();
      LocalTopologyManager ltm0 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      int preJoinTopologyId = ltm0.getCacheTopology(CACHE_NAME).getTopologyId();

      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      addBlockingLocalTopologyManager(manager(0), checkPoint, preJoinTopologyId);

      final AdvancedCache<Object, Object> cache1 = advancedCache(1);
      addBlockingLocalTopologyManager(manager(1), checkPoint, preJoinTopologyId);

      // Add a new member and block the rebalance before the final topology is installed
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      CountDownLatch stateTransferLatch = new CountDownLatch(1);
      if (op.getPreviousValue() != null) {
         c.customInterceptors().addInterceptor()
               .before(EntryWrappingInterceptor.class)
               .interceptor(new StateTransferLatchInterceptor(stateTransferLatch));
      } else {
         stateTransferLatch.countDown();
      }
      addClusterEnabledCacheManager(c);
      addBlockingLocalTopologyManager(manager(2), checkPoint, preJoinTopologyId);


      log.tracef("Starting the cache on the joiner");
      final AdvancedCache<Object,Object> cache2 = advancedCache(2);
      int duringJoinTopologyId = preJoinTopologyId + 1;

      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(0));
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(2));

      // Wait for the write CH to contain the joiner everywhere
      eventually(() -> cache0.getRpcManager().getMembers().size() == 3 &&
            cache1.getRpcManager().getMembers().size() == 3 &&
            cache2.getRpcManager().getMembers().size() == 3);

      CacheTopology duringJoinTopology = ltm0.getCacheTopology(CACHE_NAME);
      assertEquals(duringJoinTopologyId, duringJoinTopology.getTopologyId());
      assertNotNull(duringJoinTopology.getPendingCH());
      log.tracef("Rebalance started. Found key %s with current owners %s and pending owners %s", key,
            duringJoinTopology.getCurrentCH().locateOwners(key), duringJoinTopology.getPendingCH().locateOwners(key));

      // We need to wait for the state transfer to insert the entry before inserting the blocking interceptor;
      // otherwise we could block the PUT_FOR_STATE_TRANSFER instead
      stateTransferLatch.await(10, TimeUnit.SECONDS);

      // Every operation command will be blocked before reaching the distribution interceptor on cache1
      CyclicBarrier beforeCache1Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor1 = new BlockingInterceptor<>(beforeCache1Barrier,
            op.getCommandClass(), false, false);
      cache1.getAsyncInterceptorChain().addInterceptorBefore(blockingInterceptor1, TriangleDistributionInterceptor.class);

      // Every operation command will be blocked after returning to the distribution interceptor on cache2
      CyclicBarrier afterCache2Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor2 = new BlockingInterceptor<>(afterCache2Barrier,
            op.getCommandClass(), true, false,
            cmd -> !(cmd instanceof FlagAffectedCommand) || !((FlagAffectedCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER));
      cache2.getAsyncInterceptorChain().addInterceptorBefore(blockingInterceptor2, StateTransferInterceptor.class);

      // Put from cache0 with cache0 as primary owner, cache2 will become the primary owner for the retry
      Future<Object> future = fork(() -> perform(op, cache0, key));

      // Wait for the command to be executed on cache2 and unblock it
      afterCache2Barrier.await(10, TimeUnit.SECONDS);
      afterCache2Barrier.await(10, TimeUnit.SECONDS);

      // Allow the topology updates to proceed on all the caches
      for (int i = 1; i <= 3; ++i) {
         int postJoinTopologyId = duringJoinTopologyId + i;
         checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(0));
         checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(1));
         checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(2));
      }

      // Wait for the topology to change everywhere
      TestingUtil.waitForNoRebalance(cache0, cache1, cache2);

      // Allow the put command to throw an OutdatedTopologyException on cache1
      log.tracef("Unblocking the put command on node " + address(1));
      beforeCache1Barrier.await(10, TimeUnit.SECONDS);
      beforeCache1Barrier.await(10, TimeUnit.SECONDS);

      // Allow the retry to proceed on cache1
      CacheTopology postJoinTopology = ltm0.getCacheTopology(CACHE_NAME);
      if (postJoinTopology.getCurrentCH().locateOwners(key).contains(address(1))) {
         beforeCache1Barrier.await(10, TimeUnit.SECONDS);
         beforeCache1Barrier.await(10, TimeUnit.SECONDS);
      }
      // And allow the retry to finish successfully on cache2
      afterCache2Barrier.await(10, TimeUnit.SECONDS);
      afterCache2Barrier.await(10, TimeUnit.SECONDS);

      // Check that the write command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValueWithRetry(), result);
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

   protected Object perform(TestWriteOperation op, AdvancedCache<Object, Object> cache0, String key) {
      return op.perform(cache0, key);
   }

   private static class CustomConsistentHashFactory extends BaseControlledConsistentHashFactory.Default {
      private CustomConsistentHashFactory() {
         super(1);
      }

      @Override
      protected int[][] assignOwners(int numSegments, int numOwners, List<Address> members) {
         assertEquals(2, numOwners);
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
                                                final int currentTopologyId)
         throws InterruptedException {
      LocalTopologyManager component = TestingUtil.extractGlobalComponent(manager, LocalTopologyManager.class);
      LocalTopologyManager spyLtm = spy(component);
      doAnswer(invocation -> {
         CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
         // Ignore the first topology update on the joiner, which is with the topology before the join
         if (topology.getTopologyId() != currentTopologyId) {
            checkPoint.trigger("pre_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress());
            checkPoint.awaitStrict("allow_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress(),
                  10, TimeUnit.SECONDS);
         }
         return invocation.callRealMethod();
      }).when(spyLtm).handleTopologyUpdate(eq(CacheContainer.DEFAULT_CACHE_NAME), any(CacheTopology.class),
                                           any(AvailabilityMode.class), anyInt(), any(Address.class));
      TestingUtil.replaceComponent(manager, LocalTopologyManager.class, spyLtm, true);
   }

   private class StateTransferLatchInterceptor extends DDAsyncInterceptor {
      private final CountDownLatch latch;

      private StateTransferLatchInterceptor(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            if (((PutKeyValueCommand) rCommand).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               latch.countDown();
            }
         });
      }
   }
}
