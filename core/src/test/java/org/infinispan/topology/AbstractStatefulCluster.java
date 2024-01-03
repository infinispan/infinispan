package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;

public abstract class AbstractStatefulCluster extends MultipleCacheManagersTest {

   protected int clusterSize = 3;
   protected String cacheName = "clusterTestCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManager(true);
   }

   protected void createStatefulCacheManager(boolean clear) {
      for (int i = 0; i < clusterSize; i++) {
         createStatefulCacheManager(clear, Character.toString('A' + i));
      }
   }

   protected void createStatefulCacheManager(boolean clear, String id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear) Util.recursiveFileRemove(stateDirectory);

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      EmbeddedCacheManager ecm = addClusterEnabledCacheManager(global, null);

      ConfigurationBuilder builder = createCacheConfig(id);
      if (builder != null) {
         ecm.defineConfiguration(cacheName, builder.build());
      }
   }

   protected ConfigurationBuilder createCacheConfig(String id) {
      return null;
   }

   protected final void assertClusterStateFiles() throws IOException {
      assertClusterStateFiles(null);
   }

   protected final void assertClusterStateFiles(String cacheName) throws IOException {
      for (int i = 0; i < clusterSize; i++) {
         assertClusterStateFiles(manager(i), cacheName);
      }
   }

   protected final void assertClusterStateFiles(EmbeddedCacheManager ecm, String cacheName) throws IOException {
      String persistentLocation = ecm.getCacheManagerConfiguration().globalState().persistentLocation();

      boolean globalState = false;
      boolean cacheState = cacheName == null;

      try (Stream<Path> s = Files.list(Path.of(persistentLocation))) {
         Path[] paths = s.toArray(Path[]::new);


         for (Path p : paths) {
            globalState |= p.endsWith("___global.state");
            cacheState |= p.endsWith(cacheName + ".state");
         }
      }

      assertThat(globalState).as("Global state present for " + ecm.getAddress()).isTrue();
      assertThat(cacheState).as(cacheName + " state present for " + ecm.getAddress()).isTrue();
   }
}
