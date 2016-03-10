package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.cache.impl.SimpleCacheImpl;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.CustomInterceptorConfigTest;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

      assertTrue(cm.getCache() instanceof SimpleCacheImpl);
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
      new ConfigurationBuilder().simpleCache(true).storeAsBinary().enabled(true).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testCompatibility() {
      new ConfigurationBuilder().simpleCache(true).compatibility().enabled(true).build();
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
