package org.infinispan.configuration;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * LocalConfigurationValidationTest.
 *
 * @author William Burns
 * @since 7.0
 */
@Test(groups = "functional", testName = "configuration.LocalConfigurationValidation2Test")
public class LocalConfigurationValidation2Test extends SingleCacheManagerTest {

   public void testCacheModeConfiguration() {
      cacheManager.getCache().put("key", "value");
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testWrongCacheModeConfiguration() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_ASYNC);
      cacheManager.defineConfiguration("repl_async", cb.build());
      cacheManager.getCache("repl_async");
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.LOCAL);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gcb, cb);
      return cm;
   }
}