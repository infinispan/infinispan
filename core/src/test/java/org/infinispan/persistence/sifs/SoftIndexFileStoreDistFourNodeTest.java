package org.infinispan.persistence.sifs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.nio.file.Paths;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreDistFourNodeTest")
public class SoftIndexFileStoreDistFourNodeTest extends MultipleCacheManagersTest {
   private static final int CLUSTER_SIZE = 4;
   private static final String CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         createStatefulCacheManager("A" + i);
      }
   }

   private void createStatefulCacheManager(String id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.globalState().enabled(true).configurationStorage(ConfigurationStorage.OVERLAY).persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      config.persistence().addSoftIndexFileStore()
            .dataLocation(Paths.get(stateDirectory, "data").toString())
            .indexLocation(Paths.get(stateDirectory, "index").toString());
      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, config.build());
      waitForClusterToForm(CACHE_NAME);
   }

   public void testLargeClusterSize() {
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertThat(cache(i, CACHE_NAME).size()).isZero();
      }
   }
}
