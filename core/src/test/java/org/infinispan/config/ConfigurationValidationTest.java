package org.infinispan.config;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.config.Configuration.CacheMode.*;

/**
 * ConfigurationValidationTest.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "config.ConfigurationValidationTest")
public class ConfigurationValidationTest extends AbstractInfinispanTest {

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testWrongCacheModeConfiguration() throws Exception {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = createCacheManager();
         cacheManager.getCache().put("key", "value");
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testCacheModeConfiguration() throws Exception {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = createCacheManager();
         cacheManager.getCache("local").put("key", "value");
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   @Test (expectedExceptions = CacheConfigurationException.class)
   public void testDistAndReplQueue() {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setCacheMode(DIST_ASYNC);
         c.setUseReplQueue(true);
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   @Test (expectedExceptions = CacheConfigurationException.class)
   public void testEvictionOnButWithoutMaxEntries() {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setEvictionStrategy("LRU");
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   private EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration gc = GlobalConfiguration.getNonClusteredDefault();
      Configuration config = new Configuration();
      config.setCacheMode(REPL_ASYNC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gc, config);
      config = new Configuration();
      config.setCacheMode(LOCAL);
      cm.defineConfiguration("local", config);
      return cm;
   }
}
