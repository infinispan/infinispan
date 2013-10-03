package org.infinispan.configuration;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testDistAndReplQueue() {
      EmbeddedCacheManager ecm = null;
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         c.clustering().cacheMode(CacheMode.DIST_ASYNC).async().useReplQueue(true);
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testEvictionOnButWithoutMaxEntries() {
      EmbeddedCacheManager ecm = null;
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         c.eviction().strategy(EvictionStrategy.LRU);
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Indexing can not be enabled on caches in Invalidation mode")
   public void testIndexingOnInvalidationCache() {
      EmbeddedCacheManager ecm = null;
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         c.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
         c.indexing().enable();
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp =
         "ISPN(\\d)*: Indexing can only be enabled if infinispan-query.jar is available on your classpath, and this jar has not been detected.")
   public void testIndexingRequiresOptionalModule() {
      EmbeddedCacheManager ecm = null;
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         c.indexing().enable();
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   private EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.REPL_ASYNC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(config);
      config = new ConfigurationBuilder();
      cm.defineConfiguration("local", config.build());
      return cm;
   }
}
