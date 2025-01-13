package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Test for ISPN-2362 and ISPN-2502 in distributed mode. Uses a cluster which initially has 3 nodes and
 * the second node is killed in order to cause a state transfer and then test consistency.
 * Tests several operations both in an optimistic tx cluster (with write-skew check enabled) and in a pessimistic tx one.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.DistStateTransferOnLeaveConsistencyTest")
@CleanupAfterMethod
public class DistStateTransferOnLeaveConsistencyTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(DistStateTransferOnLeaveConsistencyTest.class);
   private ControlledConsistentHashFactory consistentHashFactory;

   private enum Operation {
      REMOVE, CLEAR, PUT, PUT_MAP, PUT_IF_ABSENT, REPLACE
   }

   @Override
   protected final void createCacheManagers() {
      // cache managers will be created by each test
   }

   protected ConfigurationBuilder createConfigurationBuilder(boolean isOptimistic) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      if (isOptimistic) {
         builder.transaction().lockingMode(LockingMode.OPTIMISTIC)
               .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      } else {
         builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      // Make it impossible for a key to be owned by nodes 0 and 2
      consistentHashFactory = new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}, {1, 2}});
      builder.clustering().hash().numOwners(2).numSegments(2).consistentHashFactory(consistentHashFactory);
      builder.clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);
      builder.clustering().l1().disable().locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      return builder;
   }

   public void testRemoveOptimistic() throws Exception {
      testOperationDuringLeave(Operation.REMOVE, true);
   }

   public void testRemovePessimistic() throws Exception {
      testOperationDuringLeave(Operation.REMOVE, false);
   }

   public void testClearOptimistic() throws Exception {
      testOperationDuringLeave(Operation.CLEAR, true);
   }

   public void testClearPessimistic() throws Exception {
      testOperationDuringLeave(Operation.CLEAR, false);
   }

   public void testPutOptimistic() throws Exception {
      testOperationDuringLeave(Operation.PUT, true);
   }

   public void testPutPessimistic() throws Exception {
      testOperationDuringLeave(Operation.PUT, false);
   }

   public void testPutMapOptimistic() throws Exception {
      testOperationDuringLeave(Operation.PUT_MAP, true);
   }

   public void testPutMapPessimistic() throws Exception {
      testOperationDuringLeave(Operation.PUT_MAP, false);
   }

   public void testPutIfAbsentOptimistic() throws Exception {
      testOperationDuringLeave(Operation.PUT_IF_ABSENT, true);
   }

   public void testPutIfAbsentPessimistic() throws Exception {
      testOperationDuringLeave(Operation.PUT_IF_ABSENT, false);
   }

   public void testReplaceOptimistic() throws Exception {
      testOperationDuringLeave(Operation.REPLACE, true);
   }

   public void testReplacePessimistic() throws Exception {
      testOperationDuringLeave(Operation.REPLACE, false);
   }

   private void testOperationDuringLeave(Operation op, boolean isOptimistic) throws Exception {
      ConfigurationBuilder builder = createConfigurationBuilder(isOptimistic);

      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, builder, 3);
      waitForClusterToForm();

      final int numKeys = 5;
      log.infof("Putting %d keys into cache ..", numKeys);
      for (int i = 0; i < numKeys; i++) {
         cache(0).put(i, "before_st_" + i);
      }
      log.info("Finished putting keys");

      for (int i = 0; i < numKeys; i++) {
         assertEquals("before_st_" + i, cache(0).get(i));
         assertEquals("before_st_" + i, cache(1).get(i));
         assertEquals("before_st_" + i, cache(2).get(i));
      }

      CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      CountDownLatch applyStateStartedLatch1 = new CountDownLatch(1);
      blockStateTransfer(advancedCache(0), applyStateStartedLatch1, applyStateProceedLatch);

      CountDownLatch applyStateStartedLatch2 = new CountDownLatch(1);
      blockStateTransfer(advancedCache(2), applyStateStartedLatch2, applyStateProceedLatch);

      // The indexes will only be used after node 1 is killed
      consistentHashFactory.setOwnerIndexes(new int[][]{{0, 1}, {1, 0}});
      log.info("Killing node 1 ..");
      TestingUtil.killCacheManagers(manager(1));
      log.info("Node 1 killed");

      DataContainer<Object, Object> dc0 = advancedCache(0).getDataContainer();
      DataContainer<Object, Object> dc2 = advancedCache(2).getDataContainer();

      // wait for state transfer on nodes A and C to progress to the point where data segments are about to be applied
      if (!applyStateStartedLatch1.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }
      if (!applyStateStartedLatch2.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      if (op == Operation.CLEAR) {
         log.info("Clearing cache ..");
         cache(0).clear();
         log.info("Finished clearing cache");

         assertEquals(0, dc0.size());
         assertEquals(0, dc2.size());
      } else if (op == Operation.REMOVE) {
         log.info("Removing all keys one by one ..");
         for (int i = 0; i < numKeys; i++) {
            cache(0).remove(i);
         }
         log.info("Finished removing keys");

         assertEquals(0, dc0.size());
         assertEquals(0, dc2.size());
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE || op == Operation.PUT_IF_ABSENT) {
         log.info("Updating all keys ..");
         if (op == Operation.PUT) {
            for (int i = 0; i < numKeys; i++) {
               cache(0).put(i, "after_st_" + i);
            }
         } else if (op == Operation.PUT_MAP) {
            Map<Integer, String> toPut = new HashMap<>();
            for (int i = 0; i < numKeys; i++) {
               toPut.put(i, "after_st_" + i);
            }
            cache(0).putAll(toPut);
         } else if (op == Operation.REPLACE) {
            for (int i = 0; i < numKeys; i++) {
               String expectedOldValue = "before_st_" + i;
               boolean replaced = cache(0).replace(i, expectedOldValue, "after_st_" + i);
               assertTrue(replaced);
            }
         } else { // PUT_IF_ABSENT
            for (int i = 0; i < numKeys; i++) {
               String expectedOldValue = "before_st_" + i;
               Object prevValue = cache(0).putIfAbsent(i, "after_st_" + i);
               assertEquals(expectedOldValue, prevValue);
            }
         }
         log.info("Finished updating keys");
      }

      // allow state transfer to apply state
      applyStateProceedLatch.countDown();

      // wait for apply state to end
      TestingUtil.waitForNoRebalance(cache(0), cache(2));

      // at this point state transfer is fully done
      log.tracef("Data container of NodeA has %d keys: %s", dc0.size(), StreamSupport.stream(dc0.spliterator(), false).map(ice -> ice.getKey().toString()).collect(Collectors.joining(",")));
      log.tracef("Data container of NodeC has %d keys: %s", dc2.size(), StreamSupport.stream(dc2.spliterator(), false).map(ice -> ice.getKey().toString()).collect(Collectors.joining(",")));

      if (op == Operation.CLEAR || op == Operation.REMOVE) {
         // caches should be empty. check that no keys were revived by an inconsistent state transfer
         for (int i = 0; i < numKeys; i++) {
            assertNull(dc0.get(i));
            assertNull(dc2.get(i));
         }
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE) {
         LocalizedCacheTopology cacheTopology = advancedCache(0).getDistributionManager().getCacheTopology();
         // check that all values are the ones expected after state transfer
         for (int i = 0; i < numKeys; i++) {
            // check number of owners
            int owners = 0;
            if (dc0.get(i) != null) {
               owners++;
            }
            if (dc2.get(i) != null) {
               owners++;
            }
            assertEquals("Wrong number of owners", cacheTopology.getDistribution(i).readOwners().size(), owners);

            // check values were not overwritten with old values carried by state transfer
            String expected = "after_st_" + i;
            assertEquals(expected, cache(0).get(i));
            assertEquals("after_st_" + i, cache(2).get(i));
         }
      } else { // PUT_IF_ABSENT
         LocalizedCacheTopology cacheTopology = advancedCache(0).getDistributionManager().getCacheTopology();
         for (int i = 0; i < numKeys; i++) {
            // check number of owners
            int owners = 0;
            if (dc0.get(i) != null) {
               owners++;
            }
            if (dc2.get(i) != null) {
               owners++;
            }
            assertEquals("Wrong number of owners", cacheTopology.getDistribution(i).readOwners().size(), owners);

            String expected = "before_st_" + i;
            assertEquals(expected, cache(0).get(i));
            assertEquals(expected, cache(2).get(i));
         }
      }
   }

   private static void blockStateTransfer(Cache<?,?> cache, CountDownLatch started, CountDownLatch proceed) {
      TestingUtil.wrapComponent(cache, StateConsumer.class, (current) -> {
         BlockingStateConsumer stateConsumer;
         if (current instanceof BlockingStateConsumer) {
            stateConsumer = (BlockingStateConsumer) current;
         } else {
            stateConsumer = new BlockingStateConsumer(current);
         }
         stateConsumer.startedLatch = started;
         stateConsumer.proceedLatch = proceed;
         return stateConsumer;
      });
   }

   @Scope(Scopes.NAMED_CACHE)
   public static class BlockingStateConsumer extends DelegatingStateConsumer {

      @Inject BlockingManager blockingManager;
      volatile CountDownLatch startedLatch;
      volatile CountDownLatch proceedLatch;

      BlockingStateConsumer(StateConsumer delegate) {
         super(delegate);
      }

      @Override
      public CompletionStage<?> applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
         return blockingManager.runBlocking(() -> {
            // signal we encounter a state transfer PUT
            startedLatch.countDown();
            // wait until it is ok to apply state
            try {
               if (!proceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw CompletableFutures.asCompletionException(new TimeoutException());
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            CompletionStages.join(super.applyState(sender, topologyId, stateChunks));
         }, "state-" + sender);
      }
   }
}
