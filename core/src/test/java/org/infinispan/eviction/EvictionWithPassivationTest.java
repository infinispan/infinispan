package org.infinispan.eviction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(groups = "functional", testName = "eviction.EvictionWithPassivationTest")
public class EvictionWithPassivationTest extends SingleCacheManagerTest {

   public EvictionWithPassivationTest() {
      // Cleanup needs to be after method, else LIRS can cause failures due to it not caching values due to hot
      // size being equal to full container size
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private final int EVICTION_MAX_ENTRIES = 2;

   private ConfigurationBuilder buildCfg(EvictionThreadPolicy threadPolicy, EvictionStrategy strategy) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
         .persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).purgeOnStartup(true)
         .invocationBatching().enable();
      cfg.eviction().strategy(strategy);
      // If the strategy is NONE then don't use thread policy or strategy or max entries (forces default strategy)
      if (strategy != EvictionStrategy.NONE) {
         cfg.eviction().threadPolicy(threadPolicy).maxEntries(EVICTION_MAX_ENTRIES);
      }
      return cfg;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));

      for (EvictionStrategy s : EvictionStrategy.values()) {
         for (EvictionThreadPolicy p : EvictionThreadPolicy.values()) {
            cacheManager.defineConfiguration("test-" + p + "-" + s, buildCfg(p, s).build());
         }
      }

      return cacheManager;
   }

   public void testNONE() {
      // None doesn't use eviction policy so just using DEFAULT
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE);
   }

   public void testPiggybackLRU() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU);
   }


   public void testPiggybackLIRS() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS);
   }

   public void testPiggybackUNORDERED() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED);
   }

   public void testDefaultLRU() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU);
   }


   public void testDefaultLIRS() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS);
   }

   public void testDefaultUNORDERED() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED);
   }

   public void testActivationInBatchRolledBackNONE() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE);
   }

   public void testActivationInBatchRolledBackPiggybackLRU() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU);
   }

   public void testActivationInBatchRolledBackPiggybackLIRS() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS);
   }

   public void testActivationInBatchRolledBackPiggybackUNORDERED() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED);
   }

   public void testActivationInBatchRolledBackDefaultLRU() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU);
   }

   public void testActivationInBatchRolledBackDefaultLIRS() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS);
   }

   public void testActivationInBatchRolledBackDefaultUNORDERED() {
      testActivationInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED);
   }

   public void testActivationWithAnotherConcurrentRequestNONE() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE);
   }

   public void testActivationWithAnotherConcurrentRequestPiggybackLRU() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU);
   }


   public void testActivationWithAnotherConcurrentRequestPiggybackLIRS() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS);
   }

   public void testActivationWithAnotherConcurrentRequestPiggybackUNORDERED() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED);
   }

   public void testActivationWithAnotherConcurrentRequestDefaultLRU() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU);
   }


   public void testActivationWithAnotherConcurrentRequestDefaultLIRS() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS);
   }

   public void testActivationWithAnotherConcurrentRequestDefaultUNORDERED() throws Exception {
      testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED);
   }

   public void testActivationPendingTransactionDoesNotAffectOthersNONE() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE,
                                                          "prev-value");
   }

   public void testActivationPendingTransactionDoesNotAffectOthersPiggybackLRU() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU,
                                                          "prev-value");
   }


   public void testActivationPendingTransactionDoesNotAffectOthersPiggybackLIRS() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS,
                                                          "prev-value");
   }

   public void testActivationPendingTransactionDoesNotAffectOthersPiggybackUNORDERED() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED,
                                                          "prev-value");
   }

   public void testActivationPendingTransactionDoesNotAffectOthersDefaultLRU() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU,
                                                          "prev-value");
   }


   public void testActivationPendingTransactionDoesNotAffectOthersDefaultLIRS() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS,
                                                          "prev-value");
   }

   public void testActivationPendingTransactionDoesNotAffectOthersDefaultUNORDERED() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED,
                                                          "prev-value");
   }

   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValueNONE() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE, null);
   }

   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValuePiggybackLRU() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU, null);
   }


   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValuePiggybackLIRS() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS, null);
   }

   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValuePiggybackUNORDERED() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED, null);
   }

   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValueDefaultLRU() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU, null);
   }


   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValueDefaultLIRS() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS, null);
   }

   public void testActivationPendingTransactionDoesNotAffectOthersEmptyValueDefaultUNORDERED() throws Throwable {
      testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED, null);
   }

   public void testActivationPutAllInBatchRolledBackNONE() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE);
   }

   public void testActivationPutAllInBatchRolledBackPiggybackLRU() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU);
   }


   public void testActivationPutAllInBatchRolledBackPiggybackLIRS() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS);
   }

   public void testActivationPutAllInBatchRolledBackPiggybackUNORDERED() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED);
   }

   public void testActivationPutAllInBatchRolledBackDefaultLRU() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU);
   }


   public void testActivationPutAllInBatchRolledBackDefaultLIRS() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS);
   }

   public void testActivationPutAllInBatchRolledBackDefaultUNORDERED() throws Throwable {
      testActivationPutAllInBatchRolledBack(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED);
   }

   private void runTest(EvictionThreadPolicy p, EvictionStrategy s) {
      String name = "test-" + p + "-" + s;
      Cache<String, String> testCache = cacheManager.getCache(name);
      testCache.clear();
      testCache.put("X", "4567");
      testCache.put("Y", "4568");
      testCache.put("Z", "4569");

      if (!s.equals(EvictionStrategy.NONE)) {
         assertEquals(EVICTION_MAX_ENTRIES, testCache.getAdvancedCache().getDataContainer().size());
         assertEquals("4567", testCache.get("X"));
         assertEquals(EVICTION_MAX_ENTRIES, testCache.getAdvancedCache().getDataContainer().size());
      }

      assertEquals("4567", testCache.get("X"));
      assertEquals("4568", testCache.get("Y"));
      assertEquals("4569", testCache.get("Z"));

      for (int i = 0; i < 10; i++) {
         testCache.getAdvancedCache().startBatch();
         String k = "A" + i;
         testCache.put(k, k);
         k = "B" + i;
         testCache.put(k, k);
         testCache.getAdvancedCache().endBatch(true);
      }

      for (int i = 0; i < 10; i++) {
         String k = "A" + i;
         assertEquals(k, testCache.get(k));
         k = "B" + i;
         assertEquals(k, testCache.get(k));
      }
   }

   private void testActivationInBatchRolledBack(EvictionThreadPolicy p, EvictionStrategy s) {
      String name = "test-" + p + "-" + s;
      Cache<String, String> testCache = cacheManager.getCache(name);

      final String key = "X";
      final String value = "4567";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      assertEquals(value, testCache.get(key));
      testCache.endBatch(false);

      // The data should still be present even if a rollback occurred
      assertEquals(value, testCache.get(key));
   }


   private void testActivationWithAnotherConcurrentRequest(EvictionThreadPolicy p, EvictionStrategy s) throws Exception {
      String name = "test-" + p + "-" + s;
      final Cache<String, String> testCache = cacheManager.getCache(name);

      final String key = "Y";
      final String value = "4568";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      assertEquals(value, testCache.get(key));

      // Another thread should be able to see the data as well!
      Future<String> future = testCache.getAsync(key);

      assertEquals(value, future.get(10, TimeUnit.SECONDS));

      assertEquals(value, testCache.get(key));

      testCache.endBatch(true);

      // Lastly try the retrieval after batch was committed
      assertEquals(value, testCache.get(key));
   }

   private void testActivationPendingTransactionDoesNotAffectOthers(EvictionThreadPolicy p, EvictionStrategy s,
                                                                 String previousValue) throws Throwable {
      String name = "test-" + p + "-" + s;
      final Cache<String, String> testCache = cacheManager.getCache(name);

      testCache.clear();

      final String key = "Y";
      final String value;

      if (previousValue != null) {
         testCache.put(key, previousValue);
         value = previousValue + "4568";
      } else {
         value = "4568";
      }

      // evict so it is in the loader but not in data container
      testCache.evict(key);

      testCache.startBatch();

      try {
         if (previousValue != null) {
            assertEquals(previousValue, testCache.put(key, value));
         } else {
            assertNull(testCache.put(key, value));
         }

         // In tx we should see new value
         assertEquals(value, testCache.get(key));

         // The spawned thread shouldn't see the new value yet, should see the old one still
         Future<String> future = testCache.getAsync(key);

         if (previousValue != null) {
            assertEquals(previousValue, future.get(10000, TimeUnit.SECONDS));
         } else {
            assertNull(future.get(10, TimeUnit.SECONDS));
         }
      } catch (Throwable e) {
         testCache.endBatch(false);
         throw e;
      }

      testCache.endBatch(true);

      assertEquals(value, testCache.get(key));
   }

   private void testActivationPutAllInBatchRolledBack(EvictionThreadPolicy p, EvictionStrategy s) throws Exception {
      String name = "test-" + p + "-" + s;
      Cache<String, String> testCache = cacheManager.getCache(name);

      final String key = "X";
      final String value = "4567";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      testCache.putAll(Collections.singletonMap(key, value + "-putall"));
      testCache.endBatch(false);

      // The data should still be present even if a rollback occurred
      assertEquals(value, testCache.get(key));
   }
}