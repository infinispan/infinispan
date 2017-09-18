package org.infinispan.api;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.cache.impl.SimpleCacheImpl;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.CustomInterceptorConfigTest;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
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
      cache().getAdvancedCache().addInterceptor(new CustomInterceptorConfigTest.DummyInterceptor(), 0);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testDistributedExecutor() {
      new DefaultExecutorService(cache()).submit(() -> null);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testTransactions() {
      new ConfigurationBuilder().simpleCache(true)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testInterceptors() {
      new ConfigurationBuilder().simpleCache(true)
            .customInterceptors().addInterceptor().interceptor(new BaseCustomInterceptor()).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testBatching() {
      new ConfigurationBuilder().simpleCache(true).invocationBatching().enable(true).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000381: This configuration is not supported for simple cache")
   public void testIndexing() {
      new ConfigurationBuilder().simpleCache(true).indexing().index(Index.LOCAL).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testStoreAsBinary() {
      new ConfigurationBuilder().simpleCache(true).memory().storageType(StorageType.BINARY).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testCompatibility() {
      new ConfigurationBuilder().simpleCache(true).compatibility().enabled(true).build();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStream() throws Throwable {
      super.testLockedStream();
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
   public void testLockedStreamFunctionalCommand() throws Throwable {
      super.testLockedStreamFunctionalCommand();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamPutAll() throws Throwable {
      super.testLockedStreamPutAll();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamPutAsync() throws Throwable {
      super.testLockedStreamPutAsync();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamCompute() throws Throwable {
      super.testLockedStreamCompute();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamComputeIfPresent() throws Throwable {
      super.testLockedStreamComputeIfPresent();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamMerge() throws Throwable {
      super.testLockedStreamMerge();
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
}
