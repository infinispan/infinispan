package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Test for ISPN-2362 and ISPN-2502 in distributed mode. Uses a cluster which initially has 2 nodes
 * and then a third is added to test consistency of state transfer.
 * Tests several operations both in an optimistic tx cluster (with write-skew check enabled) and in a pessimistic tx one.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.DistStateTransferOnJoinConsistencyTest")
@CleanupAfterMethod
public class DistStateTransferOnJoinConsistencyTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(DistStateTransferOnJoinConsistencyTest.class);

   private enum Operation {
      REMOVE, CLEAR, PUT, PUT_MAP, PUT_IF_ABSENT, REPLACE
   }

   @Override
   protected final void createCacheManagers() {
      // cache managers will be created by each test
   }

   protected ConfigurationBuilder createConfigurationBuilder(boolean isOptimistic) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      builder.clustering().hash().numOwners(3).numSegments(2);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      if (isOptimistic) {
         builder.transaction().lockingMode(LockingMode.OPTIMISTIC)
               .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      } else {
         builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      builder.clustering().l1().disable().locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      builder.clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);
      return builder;
   }

   public void testRemoveOptimistic() throws Exception {
      testOperationDuringJoin(Operation.REMOVE, true, false);
   }

   public void testRemoveFunctionalOptimistic() throws Exception {
      testOperationDuringJoin(Operation.REMOVE, true, true);
   }

   public void testRemovePessimistic() throws Exception {
      testOperationDuringJoin(Operation.REMOVE, false, false);
   }

   @Test(enabled = false, description = "ISPN-8212")
   public void testRemoveFunctionalPessimistic() throws Exception {
      testOperationDuringJoin(Operation.REMOVE, false, true);
   }

   public void testClearOptimistic() throws Exception {
      testOperationDuringJoin(Operation.CLEAR, true, false);
   }

   public void testFunctionalOptimistic() throws Exception {
      testOperationDuringJoin(Operation.CLEAR, true, true);
   }

   public void testClearPessimistic() throws Exception {
      testOperationDuringJoin(Operation.CLEAR, false, false);
   }

   public void testClearFunctionalPessimistic() throws Exception {
      testOperationDuringJoin(Operation.CLEAR, false, true);
   }

   public void testPutOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT, true, false);
   }

   public void testPutFunctionalOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT, true, true);
   }

   public void testPutPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT, false, false);
   }

   public void testPutFunctionalPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT, false, true);
   }

   public void testPutMapOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_MAP, true, false);
   }

   public void testPutMapFunctionalOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_MAP, true, true);
   }

   public void testPutMapPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_MAP, false, false);
   }

   public void testPutMapFunctionalPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_MAP, false, true);
   }

   public void testPutIfAbsentOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_IF_ABSENT, true, false);
   }

   public void testPutIfAbsentFunctionalOptimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_IF_ABSENT, true, true);
   }

   public void testPutIfAbsentPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_IF_ABSENT, false, false);
   }

   public void testPutIfAbsentFunctionalPessimistic() throws Exception {
      testOperationDuringJoin(Operation.PUT_IF_ABSENT, false, true);
   }

   public void testReplaceOptimistic() throws Exception {
      testOperationDuringJoin(Operation.REPLACE, true, false);
   }

   public void testReplaceFunctionalOptimistic() throws Exception {
      testOperationDuringJoin(Operation.REPLACE, true, true);
   }

   public void testReplacePessimistic() throws Exception {
      testOperationDuringJoin(Operation.REPLACE, false, false);
   }

   @Test(enabled = false, description = "ISPN-8212")
   public void testReplaceFunctionalPessimistic() throws Exception {
      testOperationDuringJoin(Operation.REPLACE, false, true);
   }

   private void testOperationDuringJoin(Operation op, boolean isOptimistic, boolean functional) throws Exception {
      ConfigurationBuilder builder = createConfigurationBuilder(isOptimistic);

      createCluster(builder, 2);
      waitForClusterToForm();

      final int numKeys = 5;
      log.infof("Putting %d keys into cache ..", numKeys);
      for (int i = 0; i < numKeys; i++) {
         cache(0).put(i, "before_st_" + i);
      }
      log.info("Finished putting keys");

      for (int i = 0; i < numKeys; i++) {
         String expected = "before_st_" + i;
         assertValue(0, i, expected);
         assertValue(1, i, expected);
      }

      final CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      final CountDownLatch applyStateStartedLatch = new CountDownLatch(1);
      builder.customInterceptors().addInterceptor().before(InvocationContextInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            // if this 'put' command is caused by state transfer we delay it to ensure other cache operations
            // are performed first and create opportunity for inconsistencies
            if (cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               // signal we encounter a state transfer PUT
               applyStateStartedLatch.countDown();
               // wait until it is ok to apply state
               if (!applyStateProceedLatch.await(15, TimeUnit.SECONDS)) {
                  throw new TimeoutException();
               }
            }
            return super.handleDefault(ctx, cmd);
         }
      });

      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builder);
      log.info("Added a new node");

      DataContainer dc0 = advancedCache(0).getDataContainer();
      DataContainer dc1 = advancedCache(1).getDataContainer();
      DataContainer dc2 = advancedCache(2).getDataContainer();

      // wait for state transfer on node C to progress to the point where data segments are about to be applied
      if (!applyStateStartedLatch.await(15, TimeUnit.SECONDS)) {
         throw new TimeoutException();
      }

      Cache<Object, Object> cache0 = cache(0);
      if (functional) {
         cache0 = FunctionalAdvancedCache.create(cache0.getAdvancedCache());
      }
      if (op == Operation.CLEAR) {
         log.info("Clearing cache ..");
         cache0.clear();
         log.info("Finished clearing cache");

         assertEquals(0, dc0.size());
         assertEquals(0, dc1.size());
      } else if (op == Operation.REMOVE) {
         log.info("Removing all keys one by one ..");
         for (int i = 0; i < numKeys; i++) {
            cache0.remove(i);
         }
         log.info("Finished removing keys");

         assertEquals(0, dc0.size());
         assertEquals(0, dc1.size());
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE || op == Operation.PUT_IF_ABSENT) {
         log.info("Updating all keys ..");
         if (op == Operation.PUT) {
            for (int i = 0; i < numKeys; i++) {
               cache0.put(i, "after_st_" + i);
            }
         } else if (op == Operation.PUT_MAP) {
            Map<Integer, String> toPut = new HashMap<>();
            for (int i = 0; i < numKeys; i++) {
               toPut.put(i, "after_st_" + i);
            }
            cache0.putAll(toPut);
         } else if (op == Operation.REPLACE) {
            for (int i = 0; i < numKeys; i++) {
               String expectedOldValue = "before_st_" + i;
               boolean replaced = cache0.replace(i, expectedOldValue, "after_st_" + i);
               assertTrue(replaced);
            }
         } else { // PUT_IF_ABSENT
            for (int i = 0; i < numKeys; i++) {
               String expectedOldValue = "before_st_" + i;
               Object prevValue = cache0.putIfAbsent(i, "after_st_" + i);
               assertEquals(expectedOldValue, prevValue);
            }
         }
         log.info("Finished updating keys");
      }

      // allow state transfer to apply state
      applyStateProceedLatch.countDown();

      // wait for apply state to end
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2));

      // at this point state transfer is fully done
      log.infof("Data container of NodeA has %d keys: %s", dc0.size(), dc0.entrySet());
      log.infof("Data container of NodeB has %d keys: %s", dc1.size(), dc1.entrySet());
      log.infof("Data container of NodeC has %d keys: %s", dc2.size(), dc2.entrySet());

      if (op == Operation.CLEAR || op == Operation.REMOVE) {
         // caches should be empty. check that no keys were revived by an inconsistent state transfer
         for (int i = 0; i < numKeys; i++) {
            assertFalse("Found value for key " + i, dc0.containsKey(i));
            assertFalse("Found value for key " + i, dc1.containsKey(i));
            assertFalse("Found value for key " + i, dc2.containsKey(i));
         }
      } else if (op == Operation.PUT || op == Operation.PUT_MAP || op == Operation.REPLACE) {
         // check that all values are the ones expected after state transfer and were not overwritten with old values carried by state transfer
         for (int i = 0; i < numKeys; i++) {
            String expectedValue = "after_st_" + i;
            assertValue(0, i, expectedValue);
            assertValue(1, i, expectedValue);
            assertValue(2, i, expectedValue);
         }
      } else { // PUT_IF_ABSENT
         // check that all values are the ones before state transfer
         for (int i = 0; i < numKeys; i++) {
            String expectedValue = "before_st_" + i;
            assertValue(0, i, expectedValue);
            assertValue(1, i, expectedValue);
            assertValue(2, i, expectedValue);
         }
      }
   }

   private void assertValue(int cacheIndex, int key, String expectedValue) {
      InternalCacheEntry ice = cache(cacheIndex).getAdvancedCache().getDataContainer().get(key);
      assertNotNull("Found null for " + key + " in cache " + cacheIndex, ice);
      assertEquals("Did not find the expected value for " + key + " in cache " + cacheIndex, expectedValue, ice.getValue());
      assertEquals("Did not find the expected value for " + key + " in cache " + cacheIndex, expectedValue, cache(cacheIndex).get(key));
   }
}
