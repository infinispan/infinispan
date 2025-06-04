package org.infinispan.api;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.cache.impl.SimpleCacheImpl;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.stats.Stats;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "api.SimpleCacheTest")
public class SimpleCacheTest extends APINonTxTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.simpleCache(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cb);

      cache = AbstractDelegatingCache.unwrapCache(cm.getCache());
      assertTrue(cache instanceof SimpleCacheImpl);
      return cm;
   }

   public void testFindInterceptor() {
      AsyncInterceptorChain interceptorChain = extractInterceptorChain(cache());
      assertNotNull(interceptorChain);
      assertNull(interceptorChain.findInterceptorExtending(InvocationContextInterceptor.class));
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testTransactions() {
      new ConfigurationBuilder().simpleCache(true)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testBatching() {
      new ConfigurationBuilder().simpleCache(true).invocationBatching().enable(true).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000381: This configuration is not supported for simple cache")
   public void testIndexing() {
      new ConfigurationBuilder().simpleCache(true).indexing().enable().build();
   }

   @Test(dataProvider = "lockedStreamActuallyLocks", expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamActuallyLocks(BiConsumer<Cache<Object, Object>, CacheEntry<Object, Object>> consumer,
                                             boolean forEachOrInvokeAll) throws Throwable {
      super.testLockedStreamActuallyLocks(consumer, forEachOrInvokeAll);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamSetValue() {
      super.testLockedStreamSetValue();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamWithinLockedStream() {
      super.testLockedStreamWithinLockedStream();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamInvokeAllFilteredSet() {
      super.testLockedStreamInvokeAllFilteredSet();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamInvokeAllPut() {
      super.testLockedStreamInvokeAllPut();
   }

   public void testStatistics() {
      Configuration cfg = new ConfigurationBuilder().simpleCache(true).statistics().enabled(true).build();
      String name = "statsCache";
      cacheManager.defineConfiguration(name, cfg);
      Cache<Object, Object> cache = cacheManager.getCache(name);
      assertEquals(0L, cache.getAdvancedCache().getStats().getStores());
      cache.put("key", "value");
      assertEquals(1L, cache.getAdvancedCache().getStats().getStores());
   }

   public void testEvictionWithStatistics() {
      int KEY_COUNT = 5;
      Configuration cfg = new ConfigurationBuilder()
            .simpleCache(true)
            .memory().maxCount(1)
            .statistics().enable()
            .build();
      String name = "evictionCache";
      cacheManager.defineConfiguration(name, cfg);
      Cache<Object, Object> cache = cacheManager.getCache(name);
      for (int i = 0; i < KEY_COUNT; i++) {
         cache.put("key" + i, "value");
      }

      Stats stats = cache.getAdvancedCache().getStats();
      assertEquals(1, stats.getCurrentNumberOfEntriesInMemory());
      assertEquals(KEY_COUNT, stats.getStores());
      assertEquals(KEY_COUNT - 1, stats.getEvictions());
   }

   public void testPutAsyncEntry() {
      AdvancedCache<Object, Object> c = cache.getAdvancedCache();
      Metadata metadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(1))
            .lifespan(25_000)
            .maxIdle(30_000)
            .build();
      assertNull(await(c.putAsync("k", "v1", metadata)));
      assertEquals("v1", cache.get("k"));

      Metadata updatedMetadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(2))
            .lifespan(35_000)
            .maxIdle(42_000)
            .build();
      CacheEntry<Object, Object> previousEntry = await(c.putAsyncEntry("k", "v2", updatedMetadata));
      assertEquals("k", previousEntry.getKey());
      assertEquals("v1", previousEntry.getValue());
      assertNotNull(previousEntry.getMetadata());
      assertMetadata(metadata, previousEntry.getMetadata());

      CacheEntry<Object, Object> currentEntry = c.getCacheEntry("k");
      assertEquals("k", currentEntry.getKey());
      assertEquals("v2", currentEntry.getValue());
      assertNotNull(currentEntry.getMetadata());
      assertMetadata(updatedMetadata, currentEntry.getMetadata());
   }

   public void testPutIfAbsentAsyncEntry() {
      AdvancedCache<Object, Object> c = cache.getAdvancedCache();
      Metadata metadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(1))
            .lifespan(25_000)
            .maxIdle(30_000)
            .build();
      assertNull(await(c.putAsync("k", "v1", metadata)));
      assertEquals("v1", c.get("k"));

      Metadata updatedMetadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(2))
            .lifespan(35_000)
            .maxIdle(42_000)
            .build();
      CacheEntry<Object, Object> previousEntry = await(c.putIfAbsentAsyncEntry("k", "v2", updatedMetadata));
      assertEquals("k", previousEntry.getKey());
      assertEquals("v1", previousEntry.getValue());

      assertMetadata(metadata, previousEntry.getMetadata());

      CacheEntry<Object, Object> currentEntry = await(c.getCacheEntryAsync("k"));
      assertEquals("k", currentEntry.getKey());
      assertEquals("v1", currentEntry.getValue());
      assertNotNull(currentEntry.getMetadata());
      assertMetadata(metadata, currentEntry.getMetadata());
   }

   public void testRemoveAsyncEntry() {
      AdvancedCache<Object, Object> c = cache.getAdvancedCache();
      Metadata metadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(1))
            .lifespan(25_000)
            .maxIdle(30_000)
            .build();
      assertNull(await(c.putAsync("k", "v", metadata)));

      CacheEntry<Object, Object> currentEntry = await(c.getCacheEntryAsync("k"));
      assertEquals("k", currentEntry.getKey());
      assertEquals("v", currentEntry.getValue());
      assertNotNull(currentEntry.getMetadata());
      assertMetadata(metadata, currentEntry.getMetadata());

      CacheEntry<Object, Object> previousEntry = await(c.removeAsyncEntry("k"));
      assertEquals("k", previousEntry.getKey());
      assertEquals("v", previousEntry.getValue());

      assertMetadata(metadata, previousEntry.getMetadata());
      assertNull(c.get("k"));

      assertNull(await(c.removeAsyncEntry("k")));
   }

   public void testReplaceAsyncEntryNonExistingKey() {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(1))
            .lifespan(25_000)
            .maxIdle(30_000)
            .build();
      CompletableFuture<CacheEntry<Object, Object>> f = cache.getAdvancedCache().replaceAsyncEntry("k", "v", metadata);
      assertNull(await(f));
   }

   public void testComputeWithExistingValue() {
      assertNull(cache.put("k", "v"));
      assertEquals("v", cache.compute("k", (key, value) -> value));
   }

   public void testReplaceAsyncEntryExistingKey() {
      AdvancedCache<Object, Object> c = cache.getAdvancedCache();
      Metadata metadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(1))
            .lifespan(25_000)
            .maxIdle(30_000)
            .build();
      assertNull(await(c.putAsync("k", "v1", metadata)));

      Metadata updatedMetadata = new EmbeddedMetadata.Builder()
            .version(new NumericVersion(2))
            .lifespan(35_000)
            .maxIdle(42_000)
            .build();
      CacheEntry<Object, Object> previousEntry = await(c.replaceAsyncEntry("k", "v2", updatedMetadata));
      assertEquals("k", previousEntry.getKey());
      assertEquals("v1", previousEntry.getValue());
      assertMetadata(metadata, previousEntry.getMetadata());

      CacheEntry<Object, Object> currentEntry = await(c.getCacheEntryAsync("k"));
      assertEquals("k", currentEntry.getKey());
      assertEquals("v2", currentEntry.getValue());
      assertNotNull(currentEntry.getMetadata());
      assertMetadata(updatedMetadata, currentEntry.getMetadata());
   }

   private void assertMetadata(Metadata expected, Metadata actual) {
      assertEquals(expected.version(), actual.version());
      assertEquals(expected.lifespan(), actual.lifespan());
      assertEquals(expected.maxIdle(), actual.maxIdle());
   }

   public void testGetGroup() {
      assertEquals(0, cache.getAdvancedCache().getGroup("group").size());
   }

   public void testGetAvailability() {
      assertEquals(AvailabilityMode.AVAILABLE, cache.getAdvancedCache().getAvailability());
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testSetAvailability() {
      cache.getAdvancedCache().setAvailability(AvailabilityMode.DEGRADED_MODE);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000696:.*")
   public void testQuery() {
      cache.query("some query");
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000696:.*")
   public void testContinuousQuery() {
      cache.continuousQuery();
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000378:.*")
   public void testStartBatch() {
      cache.startBatch();
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000378:.*")
   public void testEndBatch() {
      cache.endBatch(true);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "ISPN000377:.*")
   public void testLock() {
      cache.getAdvancedCache().lock(k());
   }

   @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "ISPN000377:.*")
   public void testLockCollection() {
      cache.getAdvancedCache().lock(List.of(k()));
   }
}
