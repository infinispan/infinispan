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

@Test(testName = "manager.CacheManagerAdminTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
   }

   public void testClusterCacheTest() {
      waitForClusterToForm();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();
      manager(0).administration().createCache("a", configuration);

      waitForClusterToForm("a");

      checkConsistencyAcrossCluster(configuration);

      addClusterEnabledCacheManager();

      checkConsistencyAcrossCluster(configuration);

      manager(1).administration().removeCache("a");

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
