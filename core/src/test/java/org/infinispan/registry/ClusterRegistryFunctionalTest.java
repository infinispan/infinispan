package org.infinispan.registry;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.registry.impl.ClusterRegistryImpl;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;


/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups="functional", testName="registry.ClusterRegistryFunctionalTest")
public class ClusterRegistryFunctionalTest extends MultipleCacheManagersTest {

   private static final int NUM_MEMBERS = 2;

   private ClusterRegistry<String, String, Integer> clusterRegistry0;
   private ClusterRegistry<String, String, Integer> clusterRegistry1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = buildConfiguration();
      createCluster(dcc, NUM_MEMBERS);
      waitForClusterToForm();
      clusterRegistry0 = manager(0).getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
      clusterRegistry1 = manager(1).getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
   }

   private ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().stateTransfer().awaitInitialTransfer(false);
      return dcc;
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
      assertTrue(clusterRegistry0.keys("noSuchScope").isEmpty());
      assertTrue(clusterRegistry1.keys("noSuchScope").isEmpty());

      //test remove
      assertNull(clusterRegistry1.get(scope, "noSuchKey"));
      clusterRegistry1.remove(scope, "noSuchKey");
      assertEquals(1, (int) clusterRegistry1.get(scope, "k1"));
      clusterRegistry1.remove(scope, "k1");
      assertNull(clusterRegistry1.get(scope, "k1"));
      assertNull(clusterRegistry0.get(scope, "k1"));

      //test clear
      clusterRegistry1.clearAll();
      assertTrue(clusterRegistry0.keys(scope).isEmpty());
      assertTrue(clusterRegistry1.keys(scope).isEmpty());
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
      assertTrue(keys.contains("k1"));
      assertTrue(keys.contains("k2"));
      assertTrue(keys.contains("k3"));
   }

   public void testListeners() {
      String scope1 = "s1", scope2 = "s2";

      TestRegistryListener listener0s1 = new TestRegistryListener();
      TestRegistryListener listener1s1 = new TestRegistryListener();
      TestRegistryListener listener0s2 = new TestRegistryListener();
      TestRegistryListener listener1s2 = new TestRegistryListener();

      clusterRegistry0.addListener(scope1, listener0s1);
      clusterRegistry1.addListener(scope1, listener1s1);
      clusterRegistry0.addListener(scope2, listener0s2);
      clusterRegistry1.addListener(scope2, listener1s2);

      clusterRegistry0.put(scope1, "k1", 1);
      clusterRegistry1.put(scope2, "k1", 2);

      assertEquals(1, (int) listener0s1.getLastUpdate(scope1, "k1"));
      assertEquals(1, (int) listener1s1.getLastUpdate(scope1, "k1"));
      assertEquals(2, (int) listener0s2.getLastUpdate(scope2, "k1"));
      assertEquals(2, (int) listener1s2.getLastUpdate(scope2, "k1"));

      // remove listeners and assert no further events are received
      clusterRegistry0.removeListener(listener0s1);
      clusterRegistry1.removeListener(listener1s1);
      clusterRegistry0.removeListener(listener0s2);
      clusterRegistry1.removeListener(listener1s2);
      listener0s1.updates.clear();
      listener1s1.updates.clear();
      listener0s2.updates.clear();
      listener1s2.updates.clear();
      clusterRegistry0.put(scope1, "k2", 1);
      clusterRegistry1.put(scope2, "k2", 2);
      assertEquals(0, listener0s1.updates.size());
      assertEquals(0, listener1s1.updates.size());
      assertEquals(0, listener0s2.updates.size());
      assertEquals(0, listener1s2.updates.size());
   }

   public void testRebalancingDisabled() throws Exception {
      // Suspend rebalancing
      LocalTopologyManager ltm0 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      ltm0.setRebalancingEnabled(false);

      ConfigurationBuilder cb = buildConfiguration();
      EmbeddedCacheManager manager2 = addClusterEnabledCacheManager(cb);
      Cache cache2 = manager2.getCache();
      // Check that cache 3 is not in the CH
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      ClusterRegistry clusterRegistry2 = manager2.getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
      // Check that the cluster registry on the joiner works properly
      clusterRegistry2.put("s1", "k1", "v1");
      assertEquals("v1", clusterRegistry0.get("s1", "k1"));

      // Check that the cluster registry cache joined properly
      TestingUtil.waitForRehashToComplete(clusterRegistryCache(0), clusterRegistryCache(1), clusterRegistryCache(2));

      // Check that cache 3 is still not in the CH
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      // Enable rebalancing
      ltm0.setRebalancingEnabled(true);
   }

   protected Cache<Object, Object> clusterRegistryCache(int index) {
      return cache(index, ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME);
   }

   @Listener
   static class TestRegistryListener {

      public final Map<ScopedKey<String, String>, Integer> updates = new HashMap<ScopedKey<String, String>, Integer>();

      public Integer getLastUpdate(String scope, String key) {
         return updates.get(new ScopedKey<String, String>(scope, key));
      }

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<ScopedKey<String, String>, Integer> e) {
         if (!e.isPre()) {
            updates.put(e.getKey(), e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<ScopedKey<String, String>, Integer> e) {
         if (!e.isPre()) {
            updates.put(e.getKey(), e.getValue());
         }
      }
   }
}
