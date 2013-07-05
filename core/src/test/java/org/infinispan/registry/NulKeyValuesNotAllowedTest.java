package org.infinispan.registry;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "registry.NulKeyValuesNotAllowedTest")
public class NulKeyValuesNotAllowedTest extends SingleCacheManagerTest {

   private ClusterRegistry<Object,Object,Object> clusterRegistry;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(dcc);
      clusterRegistry = cacheManager.getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
      return cacheManager;
   }

   public void testClusterRegistryWorksForLocalCacheManagers() {
      clusterRegistry.put("s1","k1","v1");
      clusterRegistry.put("s1","k2","v2");
      clusterRegistry.put("s2","k1","v1");
      clusterRegistry.put("s2","k2","v2");
      assertEquals("v1", clusterRegistry.get("s1", "k1"));
      assertEquals("v2", clusterRegistry.get("s1", "k2"));
      assertEquals("v1", clusterRegistry.get("s2","k1"));
      assertEquals("v2", clusterRegistry.get("s2", "k2"));
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testPutNullValue() {
      clusterRegistry.put("s","k", null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testPutNullKey() {
      clusterRegistry.put("s",null, "v");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testPutNullScope() {
      clusterRegistry.put(null, "k", "v");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testRemoveNullScope() {
      clusterRegistry.remove(null, "k");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testRemoveNullValue() {
      clusterRegistry.remove("s", null);
   }


}
