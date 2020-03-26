package org.infinispan.manager;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "manager.CacheManagerAdminPermanentTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminPermanentTest extends CacheManagerAdminTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManager("A", false);
      createStatefulCacheManager("B", false);
   }

   protected boolean isShared() {
      return false;
   }

   protected void createStatefulCacheManager(String id, boolean clear) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).
            configurationStorage(ConfigurationStorage.OVERLAY);
      if (isShared()) {
         String sharedDirectory = tmpDirectory(this.getClass().getSimpleName(), "COMMON");
         global.globalState().sharedPersistentLocation(sharedDirectory);
      } else {
         global.globalState().sharedPersistentLocation(stateDirectory);
      }
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      addClusterEnabledCacheManager(global, builder);
   }

   public void testClusterCacheTest() {
      waitForClusterToForm();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      Configuration configuration = builder.build();

      // Create a permanent cache
      manager(0).administration().createCache("a", configuration);

      waitForClusterToForm("a");

      checkConsistencyAcrossCluster("a", configuration);

      manager(1).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache("b", configuration);

      TestingUtil.killCacheManagers(this.cacheManagers);
      cacheManagers.clear();

      createStatefulCacheManager("A", false);

      checkConsistencyAcrossCluster("a", configuration);

      createStatefulCacheManager("B", true);

      checkConsistencyAcrossCluster("a", configuration);

      checkCacheExistenceAcrossCluster("b", false);

      manager(0).administration().createCache("c", configuration);

      checkConsistencyAcrossCluster("c", configuration);

      createStatefulCacheManager("C", false);

      checkConsistencyAcrossCluster("a", configuration);
   }
}
