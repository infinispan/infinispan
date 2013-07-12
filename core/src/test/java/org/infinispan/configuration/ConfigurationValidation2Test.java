package org.infinispan.configuration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * ConfigurationValidationTest.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "config.ConfigurationValidation2Test")
public class ConfigurationValidation2Test extends SingleCacheManagerTest {

   public void testWrongCacheModeConfiguration() {
      cacheManager.getCache().put("key", "value");
   }

   public void testCacheModeConfiguration() {
      cacheManager.getCache("local").put("key", "value");
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.REPL_ASYNC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(config);
      config = new ConfigurationBuilder();
      cm.defineConfiguration("local", config.build());
      return cm;
   }
}