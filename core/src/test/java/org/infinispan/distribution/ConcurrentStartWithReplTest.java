package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * To reproduce https://jira.jboss.org/jira/browse/ISPN-428
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "distribution.ConcurrentStartWithReplTest", groups = "functional")
public class ConcurrentStartWithReplTest extends MultipleCacheManagersTest {

   private Configuration config;
   private static final String TOPOLOGY_CACHE_NAME = "TopologyCacheName";

   @Override
   protected void assertSupportedConfig() {
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      config = getDefaultClusteredConfig(getCacheMode());
      CacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      CacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);

      initStateTransfer(manager(0));
      initStateTransfer(manager(1));

      manager(0).getCache();
      manager(1).getCache();

      TestingUtil.blockUntilViewReceived(manager(0).getCache(), 2, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      TestingUtil.blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);


      manager(0).getCache().put("k","v");
      manager(0).getCache().get("k").equals("v");
      manager(1).getCache().get("k").equals("v");

      log.info("Local replication test passed!");
   }

   private void initStateTransfer(CacheManager cacheManager) {
      defineTopologyCacheConfig(cacheManager);
      Cache<Object, Object> cache = cacheManager.getCache(TOPOLOGY_CACHE_NAME);
      Object o = cache.get("view");
      if (o != null) {
         cache.replace("view", "aaa");
      } else {
         cache.put("view", "bbb");
      }
   }

   protected void defineTopologyCacheConfig(CacheManager cacheManager) {
      Configuration topologyCacheConfig = new Configuration();
      topologyCacheConfig.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      topologyCacheConfig.setSyncReplTimeout(10000);
      topologyCacheConfig.setFetchInMemoryState(true);
      cacheManager.defineConfiguration(TOPOLOGY_CACHE_NAME, topologyCacheConfig);
   }


   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.DIST_SYNC;
   }

   public void testAllFine() {
      for (int i = 0; i < 10; i++) {
            manager(0).getCache().put("k" + i, "v" + i);
      }
      for (int i = 0; i < 10; i++) {
            manager(1).getCache().put("k" + i, "v" + i);
      }      
   }
}


