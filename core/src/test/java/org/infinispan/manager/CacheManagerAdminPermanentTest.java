package org.infinispan.manager;

import java.io.File;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "manager.CacheManagerAdminPermanentTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminPermanentTest extends CacheManagerAdminTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(TestingUtil.tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManager("A", false);
      createStatefulCacheManager("B", false);
   }

   private void createStatefulCacheManager(String id, boolean clear) {
      String stateDirectory = TestingUtil.tmpDirectory(this.getClass().getSimpleName() + File.separator + id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(global, builder);
   }

   public void testClusterCacheTest() {
      waitForClusterToForm();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();

      // Create a persistent cache
      manager(0).administration().withFlags(CacheContainerAdmin.AdminFlag.PERMANENT).createCache("a", configuration);

      waitForClusterToForm("a");

      checkConsistencyAcrossCluster("a", configuration);

      manager(1).administration().createCache("b", configuration);

      TestingUtil.killCacheManagers(this.cacheManagers);
      cacheManagers.clear();

      createStatefulCacheManager("A", false);

      checkConsistencyAcrossCluster("a", configuration);

      createStatefulCacheManager("B", true);

      checkConsistencyAcrossCluster("a", configuration);

      checkCacheExistenceAcrossCluster("b", false);

      manager(0).administration().withFlags(CacheContainerAdmin.AdminFlag.PERMANENT).createCache("c", configuration);

      checkConsistencyAcrossCluster("c", configuration);

      createStatefulCacheManager("C", false);

      checkConsistencyAcrossCluster("a", configuration);
   }
}
