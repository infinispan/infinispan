package org.infinispan.eviction.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.SharedBoundedLocalContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.impl.SharedContainerSimpleCacheTest")
public class SharedContainerSimpleCacheTest extends SingleCacheManagerTest {

   private static final String CONTAINER_NAME = "shared-container";
   private static final String SMALL_CONTAINER_NAME = "small-container";
   private static final String SIMPLE_CACHE_NAME = "simple-cache";
   private static final String LOCAL_CACHE_NAME = "local-cache";
   private static final String EVICTION_CACHE_NAME = "eviction-cache";
   private static final String EVICTION_CACHE_2_NAME = "eviction-cache-2";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.containerMemoryConfiguration(CONTAINER_NAME).maxCount(10000)
            .containerMemoryConfiguration(SMALL_CONTAINER_NAME).maxCount(10);

      return TestCacheManagerFactory.createCacheManager(gcb, new ConfigurationBuilder());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.simpleCache(true)
            .statistics().enabled(true)
            .memory().evictionContainer(CONTAINER_NAME);
      cacheManager.defineConfiguration(SIMPLE_CACHE_NAME, builder.build());

      ConfigurationBuilder localBuilder = new ConfigurationBuilder();
      localBuilder.statistics().enabled(true)
            .memory().evictionContainer(CONTAINER_NAME);
      cacheManager.defineConfiguration(LOCAL_CACHE_NAME, localBuilder.build());

      ConfigurationBuilder evictionBuilder = new ConfigurationBuilder();
      evictionBuilder.simpleCache(true)
            .memory().evictionContainer(SMALL_CONTAINER_NAME);
      cacheManager.defineConfiguration(EVICTION_CACHE_NAME, evictionBuilder.build());

      ConfigurationBuilder evictionBuilder2 = new ConfigurationBuilder();
      evictionBuilder2.simpleCache(true)
            .memory().evictionContainer(SMALL_CONTAINER_NAME);
      cacheManager.defineConfiguration(EVICTION_CACHE_2_NAME, evictionBuilder2.build());
   }

   public void testSimpleCacheUsesNonSegmentedContainer() {
      Cache<String, String> cache = cacheManager.getCache(SIMPLE_CACHE_NAME);
      InternalDataContainer<?, ?> dc = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      assertInstanceOf(SharedBoundedLocalContainer.class, dc, "Expected SharedBoundedLocalContainer but got " + dc.getClass().getName());
   }

   public void testSizeIncludingExpiredWithLargeSegmentSet() {
      Cache<String, String> cache = cacheManager.getCache(SIMPLE_CACHE_NAME);

      for (int i = 0; i < 10; i++) {
         cache.put("key-" + i, "value-" + i);
      }

      InternalDataContainer<?, ?> dc = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      assertEquals(10, dc.sizeIncludingExpired());
      // This is the exact scenario from the bug - calling sizeIncludingExpired with a large IntSet
      // should not throw ArrayIndexOutOfBoundsException
      assertEquals(10, dc.sizeIncludingExpired(IntSets.immutableRangeSet(256)));
   }

   public void testGetStatsDoesNotThrow() {
      Cache<String, String> cache = cacheManager.getCache(SIMPLE_CACHE_NAME);

      for (int i = 0; i < 10; i++) {
         cache.put("key-" + i, "value-" + i);
      }

      assertNotNull(cache.getAdvancedCache().getStats());
   }

   public void testTwoCachesSharingContainer() {
      Cache<String, String> simpleCache = cacheManager.getCache(SIMPLE_CACHE_NAME);
      Cache<String, String> localCache = cacheManager.getCache(LOCAL_CACHE_NAME);

      InternalDataContainer<?, ?> simpleDc = TestingUtil.extractComponent(simpleCache, InternalDataContainer.class);
      InternalDataContainer<?, ?> localDc = TestingUtil.extractComponent(localCache, InternalDataContainer.class);

      assertInstanceOf(SharedBoundedLocalContainer.class, simpleDc, "Simple cache should use SharedBoundedLocalContainer");
      assertInstanceOf(SharedBoundedLocalContainer.class, localDc, "Local cache should use SharedBoundedLocalContainer");

      for (int i = 0; i < 10; i++) {
         simpleCache.put("simple-" + i, "value-" + i);
         localCache.put("local-" + i, "value-" + i);
      }

      assertEquals(10, simpleCache.size());
      assertEquals(10, localCache.size());

      // Clearing one cache should not affect the other
      simpleCache.clear();
      assertEquals(0, simpleCache.size());
      assertEquals(10, localCache.size());
   }

   public void testEvictionWithSimpleCache() {
      Cache<String, String> cache1 = cacheManager.getCache(EVICTION_CACHE_NAME);
      Cache<String, String> cache2 = cacheManager.getCache(EVICTION_CACHE_2_NAME);

      // Fill cache1 to the shared container limit (10)
      for (int i = 0; i < 10; i++) {
         cache1.put("key-" + i, "value-" + i);
      }
      assertEquals(10, cache1.size());

      // Inserting into cache2 must evict entries from cache1 since the shared container is full
      for (int i = 0; i < 10; i++) {
         cache2.put("key-" + i, "value-" + i);
      }
      assertTrue(cache1.size() < 10, "Inserts into cache2 should have evicted entries from cache1, but cache1 size is " + cache1.size());
      int totalSize = cache1.size() + cache2.size();
      assertTrue(totalSize <= 10, "Total size across both caches should be at most 10 but was " + totalSize);

      // Now insert more into cache1 with new keys to cause evictions from cache2
      int cache2SizeBefore = cache2.size();
      for (int i = 10; i < 20; i++) {
         cache1.put("key-" + i, "value-" + i);
      }
      assertTrue(cache2.size() < cache2SizeBefore, "Inserts into cache1 should have evicted entries from cache2, but cache2 size is " + cache2.size()
            + " (was " + cache2SizeBefore + ")");
      totalSize = cache1.size() + cache2.size();
      assertTrue(totalSize <= 10, "Total size across both caches should be at most 10 but was " + totalSize);
   }
}
