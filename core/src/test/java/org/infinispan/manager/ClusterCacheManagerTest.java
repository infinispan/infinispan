package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "manager.ClusterCacheManagerTest", groups = "functional")
@CleanupAfterMethod
public class ClusterCacheManagerTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
   }

   public void testClusterCacheTest() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();
      manager(0).cluster().createCache("a", configuration);

      waitForClusterToForm("a");

      checkConsistencyAcrossCluster(configuration);

      addClusterEnabledCacheManager();

      checkConsistencyAcrossCluster(configuration);

      manager(1).cluster().removeCache("a");

      for(EmbeddedCacheManager m : cacheManagers) {
         assertFalse("Cache 'a' still present on " + m, m.cacheExists("a"));
      }

      EmbeddedCacheManager m = addClusterEnabledCacheManager();

      assertFalse("Cache 'a' should not be present on " + m, m.cacheExists("a"));


   }

   private void checkConsistencyAcrossCluster(Configuration configuration) {
      for(EmbeddedCacheManager m : cacheManagers) {
         Configuration actualConfiguration = m.getCacheConfiguration("a");
         assertEquals(configuration, actualConfiguration);
         Cache<Object, Object> cache = m.getCache("a");
         assertEquals(cacheManagers.size(), cache.getAdvancedCache().getRpcManager().getMembers().size());
      }
   }

}
