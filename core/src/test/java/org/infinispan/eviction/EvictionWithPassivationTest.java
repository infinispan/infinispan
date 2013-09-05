package org.infinispan.eviction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "eviction.EvictionWithPassivationTest")
public class EvictionWithPassivationTest extends SingleCacheManagerTest {

   private final int EVICTION_MAX_ENTRIES = 2;

   private ConfigurationBuilder buildCfg(EvictionThreadPolicy threadPolicy, EvictionStrategy strategy) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
         .persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).purgeOnStartup(true)
         .eviction().strategy(strategy).threadPolicy(threadPolicy).maxEntries(EVICTION_MAX_ENTRIES)
         .invocationBatching().enable();
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

   public void testPiggybackLRU() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU);
   }


   public void testPiggybackLIRS() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS);
   }

   public void testPiggybackNONE() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.NONE);
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

   public void testDefaultNONE() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE);
   }

   public void testDefaultUNORDERED() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED);
   }

   private void runTest(EvictionThreadPolicy p, EvictionStrategy s) {
      String name = "test-" + p + "-" + s;
      Cache<String, String> testCache = cacheManager.getCache(name);
      testCache.clear();
      testCache.put("X", "4567");
      testCache.put("Y", "4568");
      testCache.put("Z", "4569");

      if (!s.equals(EvictionStrategy.NONE)) {
         assert EVICTION_MAX_ENTRIES == testCache.getAdvancedCache().getDataContainer().size() : "Cache size should be " + EVICTION_MAX_ENTRIES;
         testCache.get("X");
         assert EVICTION_MAX_ENTRIES == testCache.getAdvancedCache().getDataContainer().size() : "Cache size should be " + EVICTION_MAX_ENTRIES;
      }

      assert "4567".equals(testCache.get("X")) : "Failure on test " + name;
      assert "4568".equals(testCache.get("Y")) : "Failure on test " + name;
      assert "4569".equals(testCache.get("Z")) : "Failure on test " + name;

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
         assert k.equals(testCache.get(k)) : "Failure on test " + name;
         k = "B" + i;
         assert k.equals(testCache.get(k)) : "Failure on test " + name;
      }
   }

}