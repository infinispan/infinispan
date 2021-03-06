package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.function.BiConsumer;

import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.cache.impl.SimpleCacheImpl;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.CustomInterceptorConfigTest;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
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

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testAddInterceptor() {
      cache().getAdvancedCache().getAsyncInterceptorChain()
             .addInterceptor(new CustomInterceptorConfigTest.DummyInterceptor(), 0);
   }

   public void testFindInterceptor() {
      AsyncInterceptorChain interceptorChain = cache().getAdvancedCache().getAsyncInterceptorChain();
      assertNotNull(interceptorChain);
      assertNull(interceptorChain.findInterceptorExtending(InvocationContextInterceptor.class));
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testTransactions() {
      new ConfigurationBuilder().simpleCache(true)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testCustomInterceptors() {
      new ConfigurationBuilder().simpleCache(true)
                                .customInterceptors().addInterceptor().interceptor(new BaseCustomAsyncInterceptor())
                                .build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testBatching() {
      new ConfigurationBuilder().simpleCache(true).invocationBatching().enable(true).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000381: This configuration is not supported for simple cache")
   public void testIndexing() {
      new ConfigurationBuilder().simpleCache(true).indexing().enable().build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testStoreAsBinary() {
      new ConfigurationBuilder().simpleCache(true).memory().storageType(StorageType.BINARY).build();
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
      Configuration cfg = new ConfigurationBuilder().simpleCache(true).jmxStatistics().enabled(true).build();
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
            .memory().size(1)
            .jmxStatistics().enable()
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
}
