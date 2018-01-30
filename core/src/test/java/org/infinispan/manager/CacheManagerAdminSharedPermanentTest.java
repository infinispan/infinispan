package org.infinispan.manager;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 9.2
 */

@Test(testName = "manager.CacheManagerAdminSharedPermanentTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminSharedPermanentTest extends CacheManagerAdminPermanentTest {

   @Override
   protected boolean isShared() {
      return true;
   }

   public void testSharedClusterCache() {

      waitForClusterToForm();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();

      // Create a persistent cache
      manager(0).administration().withFlags(CacheContainerAdmin.AdminFlag.PERMANENT).createCache("a", configuration);
      waitForClusterToForm("a");
      checkConsistencyAcrossCluster("a", configuration);

      // shutdown
      TestingUtil.killCacheManagers(this.cacheManagers);
      cacheManagers.clear();

      // recreate the cache manager
      createStatefulCacheManager("A", true);
      createStatefulCacheManager("B", true);

      checkConsistencyAcrossCluster("a", configuration);
   }
}
