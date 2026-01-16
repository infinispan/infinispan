package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.infinispan.lifecycle.ComponentStatus.TERMINATED;
import static org.infinispan.testing.Testing.tmpDirectory;

import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "globalstate.GracefulShutdownTest")
public class GracefulShutdownTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";
   private static final int NUM_NODES = 5;

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         addCacheManager(i);
      }
   }

   private void addCacheManager(int index) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Character.toString('A' + index));

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      EmbeddedCacheManager ecm = addClusterEnabledCacheManager(global, null);
      ecm.defineConfiguration(CACHE_NAME, cb.build());
   }

   public void testShutdownStopAllCoordinator() throws Throwable {
      testShutdownStopAll(true);
   }

   public void testShutdownStopAllNonCoordinator() throws Throwable {
      testShutdownStopAll(false);
   }

   public void testJoinAfterShutdown() {
      final int stoppingNode = findNodeIndex(true);

      waitForClusterToForm(CACHE_NAME);
      populateCache();
      cache(stoppingNode, CACHE_NAME).shutdown();

      addCacheManager(NUM_NODES);

      // The cache was shutdown with the previous members.
      // The new node is able to retrieve the cache instance, but it will not accept any operations.
      // The instance will return in an already terminated state.
      Cache<String, String> newCache = cache(NUM_NODES, CACHE_NAME);
      assertThat(newCache.getStatus()).isEqualTo(TERMINATED);
      assertThatThrownBy(() -> newCache.get("1"))
            .hasMessageContaining("ISPN000323: Cache '" + CACHE_NAME + "' is in 'TERMINATED'")
            .isInstanceOf(IllegalLifecycleStateException.class);
   }

   private void testShutdownStopAll(boolean fromCoordinator) throws Throwable {
      final int stoppingNode = findNodeIndex(fromCoordinator);

      waitForClusterToForm(CACHE_NAME);
      populateCache();
      cache(stoppingNode, CACHE_NAME).shutdown();

      // No operations are allowed on the cache after shutdown.
      for (int i = 0; i < NUM_NODES; i++) {
         final Cache<?, ?> cache = cache(i, CACHE_NAME);
         assertThatThrownBy(() -> cache.get("1"))
               .hasMessageContaining("ISPN000323: Cache '" + CACHE_NAME + "' is in 'TERMINATED'")
               .isInstanceOf(IllegalLifecycleStateException.class);
      }
   }

   private int findNodeIndex(boolean coordinator) {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager ecm = manager(i);
         if (ecm.isCoordinator() == coordinator) return i;
      }
      throw new AssertionError("Matching node not found in the cluster");
   }

   private void populateCache() {
      for (int i = 0; i < 100; i++) {
         cache(0, CACHE_NAME).put(String.valueOf(i), String.valueOf(i));
      }
   }
}
