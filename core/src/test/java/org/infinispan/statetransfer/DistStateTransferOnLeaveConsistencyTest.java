package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.IsolationLevel;
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

      createCluster(builder, 3);
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

      final CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      final CountDownLatch applyStateStartedLatch1 = new CountDownLatch(1);
      advancedCache(0).addInterceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we delay it to ensure other cache operations
            // are performed first and create opportunity for inconsistencies
            if (cmd instanceof PutKeyValueCommand &&
                  ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a state transfer PUT
               applyStateStartedLatch1.countDown();
               // wait until it is ok to apply state
               if (!applyStateProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      }, 0);

      final CountDownLatch applyStateStartedLatch2 = new CountDownLatch(1);
      advancedCache(2).addInterceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we delay it to ensure other cache operations
            // are performed first and create opportunity for inconsistencies
            if (cmd instanceof PutKeyValueCommand &&
                  ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a state transfer PUT
               applyStateStartedLatch2.countDown();
               // wait until it is ok to apply state
               if (!applyStateProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      }, 0);

      // The indexes will only be used after node 1 is killed
      consistentHashFactory.setOwnerIndexes(new int[][]{{0, 1}, {1, 0}});
      log.info("Killing node 1 ..");
      TestingUtil.killCacheManagers(manager(1));
      log.info("Node 1 killed");

      DataContainer dc0 = advancedCache(0).getDataContainer();
      DataContainer dc2 = advancedCache(2).getDataContainer();

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
      log.infof("Data container of NodeA has %d keys: %s", dc0.size(), dc0.entrySet());
      log.infof("Data container of NodeC has %d keys: %s", dc2.size(), dc2.entrySet());

      if (op == Operation.CLEAR || op == Operation.REMOVE) {
         // caches should be empty. check that no keys were revived by an inconsistent state transfer
         for (int i = 0; i < numKeys; i++) {
            assertFalse(dc0.containsKey(i));
            assertFalse(dc2.containsKey(i));
         }
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE) {
         ConsistentHash ch = advancedCache(0).getComponentRegistry().getStateTransferManager().getCacheTopology().getReadConsistentHash();
         // check that all values are the ones expected after state transfer
         for (int i = 0; i < numKeys; i++) {
            // check number of owners
            int owners = 0;
            if (dc0.containsKey(i)) {
               owners++;
            }
            if (dc2.containsKey(i)) {
               owners++;
            }
            assertEquals("Wrong number of owners", ch.locateOwners(i).size(), owners);

            // check values were not overwritten with old values carried by state transfer
            String expected = "after_st_" + i;
            assertEquals(expected, cache(0).get(i));
            assertEquals("after_st_" + i, cache(2).get(i));
         }
      } else { // PUT_IF_ABSENT
         ConsistentHash ch = advancedCache(0).getComponentRegistry().getStateTransferManager().getCacheTopology().getReadConsistentHash();
         for (int i = 0; i < numKeys; i++) {
            // check number of owners
            int owners = 0;
            if (dc0.containsKey(i)) {
               owners++;
            }
            if (dc2.containsKey(i)) {
               owners++;
            }
            assertEquals("Wrong number of owners", ch.locateOwners(i).size(), owners);

            String expected = "before_st_" + i;
            assertEquals(expected, cache(0).get(i));
            assertEquals(expected, cache(2).get(i));
         }
      }
   }
}
