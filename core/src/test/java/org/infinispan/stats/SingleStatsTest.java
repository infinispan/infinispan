package org.infinispan.stats;

import static org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator.estimateSizeOverhead;
import static org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator.offHeapEntrySize;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionType;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.SingleStatsTest")
public class SingleStatsTest extends MultipleCacheManagersTest {

   private static final int OFF_HEAP_KEY_SIZE = 6;
   private static final int OFF_HEAP_VALUE_SIZE = 8;
   private static final long OFF_HEAP_SIZE = estimateSizeOverhead(offHeapEntrySize(true, false /*immortal entries*/, OFF_HEAP_KEY_SIZE, OFF_HEAP_VALUE_SIZE));

   protected final int EVICTION_MAX_ENTRIES = 3;
   protected final int TOTAL_ENTRIES = 5;
   protected StorageType storageType;
   protected EvictionType evictionType;
   protected Cache<String, String> cache;
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
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.metrics().accurateSize(true);
      addClusterEnabledCacheManager(global, cfg);
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
            // Off heap key/value size is 80 bytes
            size = estimateSizeOverhead(size * OFF_HEAP_SIZE);
            // Have to also include address count overhead
            size += estimateSizeOverhead(OffHeapConcurrentMap.INITIAL_SIZE << 3);
         }
      }

      cfg
         .statistics()
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

      // Approximate size stats are the same as the accurate size stats with DummyInMemoryStore
      // Only expiration is ignored, and we do not have expired entries
      assertEquals(TOTAL_ENTRIES, stats.getApproximateEntries());
      assertEquals(EVICTION_MAX_ENTRIES, stats.getApproximateEntriesInMemory());
      assertEquals(primaryKeysCount(cache), stats.getApproximateEntriesUnique());

      // Eviction stats with passivation can be delayed
      eventuallyEquals((long) TOTAL_ENTRIES - EVICTION_MAX_ENTRIES, () -> {
         refreshStats();
         return stats.getEvictions();
      });

      int additionalMisses = 0;
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         Cache<?, ?> skipLoaderCache = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
         String key = "key" + i;
         if (skipLoaderCache.containsKey(key)) {
            cache.evict(key);
            break;
         }
         additionalMisses++;
      }

      refreshStats();
      assertEquals(TOTAL_ENTRIES - EVICTION_MAX_ENTRIES + 1, stats.getEvictions());

      assertEquals("value1", cache.get("key1"));
      assertEquals("value2", cache.get("key2"));
      assertEquals("value1", cache.remove("key1"));
      assertEquals("value2", cache.remove("key2"));
      assertNull(cache.remove("non-existing"));
      assertNull(cache.get("key1"));
      assertNull(cache.get("key2"));

      refreshStats();
      assertEquals(3, stats.getHits());
      assertEquals(2 + additionalMisses, stats.getMisses());
      assertEquals(2, stats.getRemoveHits());
      assertEquals(1, stats.getRemoveMisses());
      assertEquals(5 + additionalMisses, stats.getRetrievals());
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
         assertEquals(-1L, stats.getDataMemoryUsed());
      } else {
         assertTrue(stats.getDataMemoryUsed() > 0);
      }
      if (storageType == StorageType.OFF_HEAP) {
         assertTrue(stats.getOffHeapMemoryUsed() > 0);
      }
   }

   protected long primaryKeysCount(Cache<?, ?> cache) {
      return TOTAL_ENTRIES;
   }

   protected void refreshStats() {
      stats = cache.getAdvancedCache().getStats();
   }
}
