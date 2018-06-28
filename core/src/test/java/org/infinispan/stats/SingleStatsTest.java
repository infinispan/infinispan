package org.infinispan.stats;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.eviction.EvictionType;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.SingleStatsTest")
public class SingleStatsTest extends MultipleCacheManagersTest {

   protected final int EVICTION_MAX_ENTRIES = 3;
   protected final int TOTAL_ENTRIES = 5;
   protected StorageType storageType;
   protected EvictionType evictionType;
   protected Cache cache;
   protected Stats stats;

   @Override
   public Object[] factory() {
      return new Object[]{
            new SingleStatsTest().withStorage(StorageType.BINARY).withEvictionType(EvictionType.MEMORY),
            new SingleStatsTest().withStorage(StorageType.BINARY).withEvictionType(EvictionType.COUNT),
            new SingleStatsTest().withStorage(StorageType.OBJECT).withEvictionType(EvictionType.COUNT),
            new SingleStatsTest().withStorage(StorageType.OFF_HEAP).withEvictionType(EvictionType.COUNT),
            new SingleStatsTest().withStorage(StorageType.OFF_HEAP).withEvictionType(EvictionType.MEMORY),
      };
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "StorageType", "EvictionType");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType, evictionType);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      configure(cfg);
      addClusterEnabledCacheManager(cfg);
      cache = cache(0);
      refreshStats();
   }

   protected void configure(ConfigurationBuilder cfg) {
      long size = EVICTION_MAX_ENTRIES;
      if (evictionType == EvictionType.MEMORY) {
         if (storageType == StorageType.BINARY) {
            // Binary key/value size is 128 bytes
            size *= 128;
         } else {
            // Off heap key/value size is 64 bytes
            size = UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(size * 64);
            // Have to also include address count overhead
            size += UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(cfg.memory().addressCount() << 3);
         }
      }

      cfg
         .jmxStatistics()
            .enable()
         .memory()
            .storageType(storageType)
            .evictionType(evictionType)
            .size(size)
         .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .purgeOnStartup(true);
   }

   public SingleStatsTest withStorage(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public SingleStatsTest withEvictionType(EvictionType evictionType) {
      this.evictionType = evictionType;
      return this;
   }

   @AfterMethod
   public void cleanCache() {
      cache.clear();
      cache.getAdvancedCache().getStats().reset();
   }

   public void testStats() {
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         cache.put("key" + i, "value" + i);
      }

      refreshStats();
      assertEquals(TOTAL_ENTRIES, stats.getCurrentNumberOfEntries());
      assertEquals(EVICTION_MAX_ENTRIES, stats.getCurrentNumberOfEntriesInMemory());
      assertEquals(TOTAL_ENTRIES - EVICTION_MAX_ENTRIES, stats.getEvictions());

      cache.evict("key0");

      refreshStats();
      assertEquals(TOTAL_ENTRIES - EVICTION_MAX_ENTRIES + 1, stats.getEvictions());

      cache.get("key1");
      cache.get("key2");
      cache.remove("key1");
      cache.remove("key2");
      cache.remove("non-existing");
      cache.get("key1");
      cache.get("key2");

      refreshStats();
      //assertEquals(2, stats.getHits()); //https://issues.jboss.org/browse/ISPN-8442
      //assertEquals(2, stats.getMisses()); //https://issues.jboss.org/browse/ISPN-8442
      assertEquals(2, stats.getRemoveHits());
      assertEquals(1, stats.getRemoveMisses());
      assertEquals(4, stats.getRetrievals());
      assertEquals(TOTAL_ENTRIES, stats.getStores());

      cache.put("other-key", "value");

      refreshStats();
      assertEquals(TOTAL_ENTRIES + 1, stats.getTotalNumberOfEntries());

      assertTrue(stats.getAverageReadTime() >= 0);
      assertTrue(stats.getAverageRemoveTime() >= 0);
      assertTrue(stats.getAverageWriteTime() >= 0);
      assertTrue(stats.getAverageReadTimeNanos() >= 0);
      assertTrue(stats.getAverageRemoveTimeNanos() >= 0);
      assertTrue(stats.getAverageWriteTimeNanos() >= 0);
      if (evictionType == EvictionType.COUNT) {
         assertEquals(0, stats.getDataMemoryUsed());
      } else {
         assertTrue(stats.getDataMemoryUsed() > 0);
      }
      if (storageType == StorageType.OFF_HEAP) {
         assertTrue(stats.getOffHeapMemoryUsed() > 0);
      }
   }

   protected void refreshStats() {
      stats = cache.getAdvancedCache().getStats();
   }
}
