package org.infinispan.globalstate;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.PersistentUUID;
import org.testng.annotations.Test;

@Test(testName = "globalstate.GlobalStateRestartTest", groups = "functional")
public class GlobalStateRestartTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createStatefulCacheManager("A", true);
      createStatefulCacheManager("B", true);
   }

   private void createStatefulCacheManager(String id, boolean clear) {
      String stateDirectory = TestingUtil.tmpDirectory(GlobalStateRestartTest.class.getSimpleName() + "_" + id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1);
      config.persistence().addSingleFileStore().location(stateDirectory);
      addClusterEnabledCacheManager(global, config);
   }

   public void testGracefulShutdownAndRestart() {
      waitForClusterToForm();
      LocalTopologyManager ltm1 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      LocalTopologyManager ltm2 = TestingUtil.extractGlobalComponent(manager(1), LocalTopologyManager.class);
      PersistentUUID uuid1 = ltm1.getPersistentUUID();
      assertNotNull(uuid1);
      PersistentUUID uuid2 = ltm2.getPersistentUUID();
      assertNotNull(uuid2);
      String gsp0_location = manager(0).getCacheManagerConfiguration().globalState().persistentLocation();
      String gsp1_location = manager(1).getCacheManagerConfiguration().globalState().persistentLocation();

      TestingUtil.killCacheManagers(this.cacheManagers);
      checkStateDirNotEmpty(gsp0_location);
      checkStateDirNotEmpty(gsp1_location);
      this.cacheManagers.clear();

      // Recreate the cluster
      createStatefulCacheManager("A", false);
      createStatefulCacheManager("B", false);
      waitForClusterToForm();
      ltm1 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      ltm2 = TestingUtil.extractGlobalComponent(manager(1), LocalTopologyManager.class);
      assertEquals(uuid1, ltm1.getPersistentUUID());
      assertEquals(uuid2, ltm2.getPersistentUUID());
   }

   private void checkStateDirNotEmpty(String gsp0_location) {
      File[] listFiles = new File(gsp0_location).listFiles();
      assertTrue(listFiles.length > 0);
   }

}