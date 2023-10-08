package org.infinispan.stats;

import static org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator.estimateSizeOverhead;
import static org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator.offHeapEntrySize;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.SingleStatsTest")
public class SingleStatsTest extends MultipleCacheManagersTest {

   private static final int OFF_HEAP_KEY_SIZE = 8 ;
   private static final int OFF_HEAP_VALUE_SIZE = 8;
   private long OFF_HEAP_SIZE = -1;

   protected final int EVICTION_MAX_ENTRIES = 3;
   protected final int TOTAL_ENTRIES = 5;
   protected StorageType storageType;
   protected boolean countBasedEviction;
   protected boolean accurateSize = true;
   protected EvictionStrategy evictionStrategy = EvictionStrategy.REMOVE;
   protected Cache<String, String> cache;
   protected Stats stats;

   @Override
   public Object[] factory() {
      return Arrays.stream(EvictionStrategy.values())
            .filter(EvictionStrategy::isEnabled)
            .flatMap(strategy ->
                  Arrays.stream(new Object[]{
                        new SingleStatsTest().withStorage(StorageType.BINARY).withCountEviction(false).withEvictionStrategy(strategy),
                        new SingleStatsTest().withStorage(StorageType.BINARY).withCountEviction(true).withEvictionStrategy(strategy),
                        new SingleStatsTest().withStorage(StorageType.BINARY).withCountEviction(true).withEvictionStrategy(strategy).withAccurateSize(false),
                        new SingleStatsTest().withStorage(StorageType.HEAP).withCountEviction(true).withEvictionStrategy(strategy),
                        new SingleStatsTest().withStorage(StorageType.HEAP).withCountEviction(true).withEvictionStrategy(strategy).withAccurateSize(false),
                        new SingleStatsTest().withStorage(StorageType.OFF_HEAP).withCountEviction(true).withEvictionStrategy(strategy),
                        new SingleStatsTest().withStorage(StorageType.OFF_HEAP).withCountEviction(false).withEvictionStrategy(strategy),
                        new SingleStatsTest().withStorage(StorageType.OFF_HEAP).withCountEviction(false).withEvictionStrategy(strategy).withAccurateSize(false),
                  })
            ).toArray();
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "StorageType", "CountBasedEviction", "EvictionStrategy", "AccurateSize");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType, countBasedEviction, evictionStrategy, accurateSize);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      configure(cfg);
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.metrics().accurateSize(accurateSize);
      addClusterEnabledCacheManager(global, cfg);
      cache = cache(0);
      refreshStats();
   }

   protected void configure(ConfigurationBuilder cfg) {
      long size = EVICTION_MAX_ENTRIES;
      MemoryConfigurationBuilder memoryConfigurationBuilder = cfg.memory();
      memoryConfigurationBuilder.storage(storageType);
      if (countBasedEviction) {
         memoryConfigurationBuilder.maxCount(size);
      } else {
         if (storageType == StorageType.OFF_HEAP) {
            // When expiration eviction is used it stores the internal metadata which we will estimate as 19 bytes (4 size and 15 for actual bytes)
            long amountPerEntry = estimateSizeOverhead(OFF_HEAP_SIZE + (evictionStrategy.isExceptionBased() ? 19 : 0));
            size = size * amountPerEntry;
            // Have to also include address count overhead
            size += estimateSizeOverhead(OffHeapConcurrentMap.INITIAL_SIZE << 3);
         } else {
            // Binary key/value size is 128 bytes
            // Exception requires transactions which adds additional overhead
            size *= (128 + (evictionStrategy.isExceptionBased() ? 48 : 0));
         }
         memoryConfigurationBuilder.maxSize(Long.toString(size));
      }

      memoryConfigurationBuilder.whenFull(evictionStrategy);
      if (evictionStrategy == EvictionStrategy.EXCEPTION) {
         cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }

      cfg
            .statistics()
            .enable()
            .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .purgeOnStartup(true);
   }

   public SingleStatsTest withStorage(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public SingleStatsTest withCountEviction(boolean countBasedEviction) {
      this.countBasedEviction = countBasedEviction;
      return this;
   }

   public SingleStatsTest withEvictionStrategy(EvictionStrategy evictionStrategy) {
      this.evictionStrategy = evictionStrategy;
      OFF_HEAP_SIZE = offHeapEntrySize(evictionStrategy == EvictionStrategy.REMOVE, false /*immortal entries*/, OFF_HEAP_KEY_SIZE, OFF_HEAP_VALUE_SIZE);
      return this;
   }

   public SingleStatsTest withAccurateSize(boolean accurateSize) {
      this.accurateSize = accurateSize;
      return this;
   }

   @AfterMethod
   public void cleanCache() {
      cache.clear();
      cache.getAdvancedCache().getStats().reset();
   }

   public void testStats() {
      refreshStats();
      if (accurateSize) {
         assertEquals(0, stats.getCurrentNumberOfEntries());
      } else {
         assertEquals(-1, stats.getCurrentNumberOfEntries());
      }

      int insertErrors = 0;
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         try {
            cache.put("key" + i, "value" + i);
         } catch (CacheException e) {
            insertErrors++;
         }
      }
      if (insertErrors > 0 && TOTAL_ENTRIES - EVICTION_MAX_ENTRIES != insertErrors) {
         fail("Number of failed errors was: " + insertErrors + " manually check them.");
      }

      DataOperationOrderer doo = TestingUtil.extractComponent(cache, DataOperationOrderer.class);
      eventually(() -> doo.pendingOperations() == 0);

      int expectedSize = TOTAL_ENTRIES - insertErrors;

      refreshStats();

      // Approximate size stats are the same as the accurate size stats with DummyInMemoryStore
      // Only expiration is ignored, and we do not have expired entries
      assertEquals(expectedSize, stats.getApproximateEntries());
      assertEquals(EVICTION_MAX_ENTRIES, stats.getApproximateEntriesInMemory());
      assertEquals(primaryKeysCount(cache), stats.getApproximateEntriesUnique());
      if (accurateSize) {
         assertEquals(expectedSize, stats.getCurrentNumberOfEntries());
         assertEquals(EVICTION_MAX_ENTRIES, stats.getCurrentNumberOfEntriesInMemory());
      } else {
         assertEquals(-1, stats.getCurrentNumberOfEntries());
         assertEquals(-1, stats.getCurrentNumberOfEntriesInMemory());
      }

      // Eviction stats with passivation can be delayed
      eventuallyEquals((long) expectedSize - EVICTION_MAX_ENTRIES, () -> {
         refreshStats();
         return stats.getEvictions();
      });

      int additionalMisses = 0;
      for (int i = 0; i < expectedSize; i++) {
         Cache<?, ?> skipLoaderCache = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
         String key = "key" + i;
         if (skipLoaderCache.containsKey(key)) {
            cache.evict(key);
            break;
         }
         additionalMisses++;
      }

      refreshStats();
      assertEquals(expectedSize - EVICTION_MAX_ENTRIES + 1, stats.getEvictions());

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
      assertEquals(TOTAL_ENTRIES + 1, stats.getStores());

      assertTrue(stats.getAverageReadTime() >= 0);
      assertTrue(stats.getAverageRemoveTime() >= 0);
      assertTrue(stats.getAverageWriteTime() >= 0);
      assertTrue(stats.getAverageReadTimeNanos() >= 0);
      assertTrue(stats.getAverageRemoveTimeNanos() >= 0);
      assertTrue(stats.getAverageWriteTimeNanos() >= 0);
      if (countBasedEviction || evictionStrategy.isExceptionBased()) {
         assertEquals(-1L, stats.getDataMemoryUsed());
      } else {
         assertTrue(stats.getDataMemoryUsed() > 0);
      }
      if (storageType == StorageType.OFF_HEAP) {
         assertTrue(stats.getOffHeapMemoryUsed() > 0);
      }
   }

   protected long primaryKeysCount(Cache<?, ?> cache) {
      return evictionStrategy == EvictionStrategy.EXCEPTION ? EVICTION_MAX_ENTRIES : TOTAL_ENTRIES;
   }

   protected void refreshStats() {
      stats = cache.getAdvancedCache().getStats();
   }
}
