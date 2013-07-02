package org.infinispan.config;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.config.Configuration.CacheMode.LOCAL;
import static org.infinispan.config.Configuration.CacheMode.REPL_ASYNC;

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
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
      Configuration config = new Configuration();
      config.setCacheMode(REPL_ASYNC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gc, config);
      config = new Configuration();
      config.setCacheMode(LOCAL);
      cm.defineConfiguration("local", config);
      return cm;
   }
}