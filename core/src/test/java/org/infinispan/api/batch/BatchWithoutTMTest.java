package org.infinispan.api.batch;

import static org.infinispan.test.fwk.TestCacheManagerFactory.getDefaultCacheConfiguration;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.Exceptions;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "api.batch.BatchWithoutTMTest")
public class BatchWithoutTMTest extends AbstractBatchTest {

   @Override
   public EmbeddedCacheManager createCacheManager() {
      final ConfigurationBuilder defaultConfiguration = getDefaultCacheConfiguration(true);
      defaultConfiguration.invocationBatching().enable().transaction().autoCommit(false);
      return TestCacheManagerFactory.createCacheManager(defaultConfiguration);
   }

   public void testBatchWithoutCfg(Method method) {
      Cache<String, String> cache = createCache(false, method.getName());
      Exceptions.expectException(CacheConfigurationException.class, cache::startBatch);
      Exceptions.expectException(CacheConfigurationException.class, () -> cache.endBatch(true));
      Exceptions.expectException(CacheConfigurationException.class, () -> cache.endBatch(false));
   }

   public void testEndBatchWithoutStartBatch(Method method) {
      Cache<String, String> cache = createCache(method.getName());
      cache.endBatch(true);
      cache.endBatch(false);
      // should not fail.
   }

   public void testStartBatchIdempotency(Method method) {
      Cache<String, String> cache = createCache(method.getName());
      cache.startBatch();
      cache.put("k", "v");
      cache.startBatch();     // again
      cache.put("k2", "v2");
      cache.endBatch(true);

      AssertJUnit.assertEquals("v", cache.get("k"));
      AssertJUnit.assertEquals("v2", cache.get("k2"));
   }

   public void testBatchVisibility(Method method) throws Exception {
      Cache<String, String> cache = createCache(method.getName());
      cache.startBatch();
      cache.put("k", "v");
      assertNull("Other thread should not see batch update till batch completes!", getOnDifferentThread(cache, "k"));

      cache.endBatch(true);

      AssertJUnit.assertEquals("v", getOnDifferentThread(cache, "k"));
   }

   public void testBatchRollback(Method method) throws Exception {
      Cache<String, String> cache = createCache(method.getName());
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      cache.endBatch(false);

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));
   }

   @Override
   protected <K, V> Cache<K, V> createCache(String name) {
      return createCache(true, name);
   }

   private <K, V> Cache<K, V> createCache(boolean enableBatch, String name) {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.invocationBatching().enable(enableBatch);
      cacheManager.defineConfiguration(name, c.build());
      return cacheManager.getCache(name);
   }
}
