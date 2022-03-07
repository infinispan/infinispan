package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * ISPN-13740 A test to ensure that a NPE is not thrown when a cache is deleted after a graceful shutdown and restart.
 *
 * The {@link org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl} uses events and the {@link org.infinispan.globalstate.impl.GlobalConfigurationStateListener}
 * to create Caches and templates cluster-wide after a local caches.xml and templates.xml have been loaded. Consequently,
 * there is a small window on a cluster restart where attempts to remove a cache/template will result in an event being
 * created with null state, as the state in the cache has not yet been created by the local listener. This test is to ensure
 * that the implementation never relies on this state during remove operations.
 *
 * @author Ryan Emerson
 * @since 14.0
 */
@Test(groups = "functional", testName = "globalstate.GlobalStateRestartWithCacheTest")
public class GlobalStateRestartWithCacheTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManagers();
   }

   private void createStatefulCacheManagers() {
      IntStream.range(0, 2).forEach(this::createStatefulCacheManager);
   }

   private void createStatefulCacheManager(int index) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Integer.toString(index));
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      addClusterEnabledCacheManager(global, null);
   }

   public void testCacheDeletionAfterRestart() {
      // Create cache via EmbeddedCacheManagerAdmin
      Configuration cacheConfig = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      manager(0).administration().createCache(CACHE_NAME, cacheConfig);
      waitForClusterToForm(CACHE_NAME);

      // Shutdown server in the same manner as the Server
      manager(0).shutdownAllCaches();
      for (EmbeddedCacheManager manager : managers()) {
         manager.stop();
      }

      // Restart the cluster
      // Verify that the cache state file exists
      for (int i = 0; i < 2; i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(Arrays.toString(listFiles), 1, listFiles.length);
      }
      this.cacheManagers.clear();

      // Recreate the cluster
      createStatefulCacheManagers();

      // Delete cache
      manager(0).administration().removeCache(CACHE_NAME);
   }

   @Override
   public DefaultCacheManager manager(int i) {
      return (DefaultCacheManager) cacheManagers.get(i);
   }
}
