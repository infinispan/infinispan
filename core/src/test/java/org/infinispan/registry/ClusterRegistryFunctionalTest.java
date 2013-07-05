package org.infinispan.registry;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.Set;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;


/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups="functional", testName="registry.ClusterRegistryFunctionalTest")
public class ClusterRegistryFunctionalTest extends MultipleCacheManagersTest {

   ClusterRegistry<String, String, Integer> clusterRegistry0;
   ClusterRegistry<String, String, Integer> clusterRegistry1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      createCluster(dcc, 3);
      waitForClusterToForm();
      clusterRegistry0 = manager(0).getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
      clusterRegistry1 = manager(1).getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
   }

   public void testSimpleFunctionality() {
      String scope = "ClusterRegistryFunctionalTest";
      clusterRegistry0.put(scope, "k1", 1);
      clusterRegistry0.put(scope, "k2", 2);
      clusterRegistry0.put(scope, "k3", 3);

      //test get
      assertEquals(1, (int) clusterRegistry0.get(scope, "k1"));
      assertEquals(1, (int) clusterRegistry1.get(scope, "k1"));

      //test keys
      testExpectedKeys(clusterRegistry0.keys(scope));
      testExpectedKeys(clusterRegistry1.keys(scope));
      org.junit.Assert.assertTrue(clusterRegistry0.keys("noSuchScope").isEmpty());
      org.junit.Assert.assertTrue(clusterRegistry1.keys("noSuchScope").isEmpty());

      //test remove
      assertNull(clusterRegistry1.remove(scope, "noSuchKey"));
      assertEquals(1, (int)clusterRegistry1.remove(scope, "k1"));
      assertNull(clusterRegistry1.get(scope, "k1"));
      assertNull(clusterRegistry0.get(scope, "k1"));

      //test clear
      clusterRegistry1.clearAll();
      org.junit.Assert.assertTrue(clusterRegistry0.keys(scope).isEmpty());
      org.junit.Assert.assertTrue(clusterRegistry1.keys(scope).isEmpty());
   }

   public void testReturnValues() {
      String scope = "ClusterRegistryFunctionalTest";
      assertNull(clusterRegistry0.put(scope, "k1", 1));
      assertEquals(1, (int) clusterRegistry1.put(scope, "k1", 2));
      assertNull(clusterRegistry0.remove(scope, "k2"));
      assertEquals(2, (int) clusterRegistry1.remove(scope, "k1"));
   }

   public void testClearAll() {
      String scope1 = "s1";
      String scope2 = "s2";
      clusterRegistry0.put(scope1, "k1", 1);
      clusterRegistry0.put(scope1, "k2", 2);

      clusterRegistry1.put(scope2, "k1", 3);
      clusterRegistry1.put(scope2, "k2", 4);
      clusterRegistry1.put(scope2, "k3", 5);

      assertEquals(2, clusterRegistry0.keys(scope1).size());
      assertEquals(2, clusterRegistry1.keys(scope1).size());
      assertEquals(3, clusterRegistry0.keys(scope2).size());
      assertEquals(3, clusterRegistry1.keys(scope2).size());

      clusterRegistry0.clearAll();
      assertEquals(0, clusterRegistry0.keys(scope1).size());
      assertEquals(0, clusterRegistry1.keys(scope1).size());
      assertEquals(0, clusterRegistry0.keys(scope2).size());
      assertEquals(0, clusterRegistry1.keys(scope2).size());
   }

   public void testClear() {
      String scope1 = "s1", scope2 = "s2";
      clusterRegistry0.put(scope1, "k1", 1);
      clusterRegistry0.put(scope1, "k2", 2);
      clusterRegistry0.put(scope1, "k3", 3);

      clusterRegistry1.clear(scope2);

      assertEquals(1, (int) clusterRegistry1.get(scope1, "k1"));
      assertEquals(2, (int) clusterRegistry1.get(scope1, "k2"));
      assertEquals(3, (int) clusterRegistry1.get(scope1, "k3"));
   }

   public void testNoCollision() {
      String scope1 = "s1", scope2 = "s2";
      clusterRegistry0.put(scope1, "k1", 1);
      clusterRegistry1.put(scope2, "k1", 2);
      assertEquals(1, (int)clusterRegistry0.get(scope1, "k1"));
      assertEquals(1, (int)clusterRegistry1.get(scope1, "k1"));
      assertEquals(2, (int)clusterRegistry0.get(scope2, "k1"));
      assertEquals(2, (int)clusterRegistry1.get(scope2, "k1"));
   }

   private void testExpectedKeys(Set<String> keys) {
      assertEquals(keys.size(), 3);
      org.junit.Assert.assertTrue(keys.contains("k1"));
      org.junit.Assert.assertTrue(keys.contains("k2"));
      org.junit.Assert.assertTrue(keys.contains("k3"));
   }
}
