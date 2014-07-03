package org.infinispan.registry;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.registry.impl.ClusterRegistryImpl;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "registry.ClusterRegistryWithTopologyChangeTest")
public class ClusterRegistryWithTopologyChangeTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder dcc;
   private ClusterRegistry<Object, Object, Object> clusterRegistry0;

   @Override
   protected void createCacheManagers() throws Throwable {
      dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      createCluster(dcc, 1);
      waitForClusterToForm();
      clusterRegistry0 = manager(0).getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
   }

   public void testNodeAdded() throws Exception {
      clusterRegistry0.put("s1","k1","v1");
      clusterRegistry0.put("s1","k2","v2");
      clusterRegistry0.put("s2","k1","v1");
      clusterRegistry0.put("s2","k2","v2");
      checkExpectedValues(clusterRegistry0);

      addClusterEnabledCacheManager(dcc);
      waitForClusterToForm();

      //the registry hasn't been started yet
      assertNull(manager(1).getCacheConfiguration(ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME));

      ClusterRegistry<Object, Object, Object> clusterRegistry1 = manager(1).getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
      checkExpectedValues(clusterRegistry0);
      checkExpectedValues(clusterRegistry1);

      assertNotNull(manager(1).getCacheConfiguration(ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME));
   }

   @Test (dependsOnMethods = "testNodeAdded")
   public void testClusterRegistryCleanedBetweenTestRuns() {
      assertTrue(clusterRegistry0.keys("s1").isEmpty());
   }

   private void checkExpectedValues(ClusterRegistry<Object, Object, Object> clusterRegistry) {
      assertEquals("v1", clusterRegistry.get("s1", "k1"));
      assertEquals("v2", clusterRegistry.get("s1", "k2"));
      assertEquals("v1", clusterRegistry.get("s2","k1"));
      assertEquals("v2", clusterRegistry.get("s2", "k2"));
   }
}
