package org.infinispan.health;

import static org.infinispan.lifecycle.ComponentStatus.INSTANTIATED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.EnumSet;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.health.impl.ClusterHealthImpl;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "health.ClusterHealthImplTest", groups = "functional")
public class ClusterHealthImplTest extends AbstractInfinispanTest {

   private static final String INTERNAL_CACHE_NAME = "internal_cache";
   private static final String CACHE_NAME = "test_cache";
   private EmbeddedCacheManager cacheManager;
   private DefaultCacheManager mockedCacheManager;
   private ClusterHealth clusterHealth;
   private InternalCacheRegistry internalCacheRegistry;

   @BeforeClass
   private void init() {
      cacheManager = TestCacheManagerFactory.createClusteredCacheManager();
      internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      clusterHealth = new ClusterHealthImpl(cacheManager);
   }

   @BeforeMethod
   private void configureBeforeMethod() {
      mockedCacheManager = mock(DefaultCacheManager.class);

      // We return the real global component registry to avoid to mock all the dependencies in the world
      when(mockedCacheManager.getGlobalComponentRegistry()).thenReturn(cacheManager.getGlobalComponentRegistry());

      internalCacheRegistry.registerInternalCache(INTERNAL_CACHE_NAME, new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_ASYNC).build(),
            EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));

      cacheManager.defineConfiguration(CACHE_NAME, new ConfigurationBuilder().build());
   }

   @AfterMethod(alwaysRun = true)
   private void cleanAfterMethod() {
      cacheManager.administration().removeCache(CACHE_NAME);
      cacheManager.undefineConfiguration(CACHE_NAME);

      cacheManager.administration().removeCache(INTERNAL_CACHE_NAME);
      internalCacheRegistry.unregisterInternalCache(INTERNAL_CACHE_NAME);
   }

   @AfterClass(alwaysRun = true)
   private void cleanUp() {
      if (cacheManager != null) {
         cacheManager.stop();
         cacheManager = null;
      }
   }

   public void testGetClusterName() throws Exception {
      assertEquals(cacheManager.getClusterName(), clusterHealth.getClusterName());
   }

   public void testCallingGetHealthStatusDoesNotCreateAnyCache() throws Exception {
      clusterHealth.getHealthStatus();

      assertFalse(cacheManager.cacheExists(CACHE_NAME));
      assertFalse(cacheManager.cacheExists(INTERNAL_CACHE_NAME));
   }

   public void testHealthyStatusWithoutAnyUserCreatedCache() throws Exception {
      assertEquals(HealthStatus.HEALTHY, clusterHealth.getHealthStatus());
   }

   public void testHealthyStatusWhenUserCacheIsHealthy() throws Exception {
      cacheManager.getCache(CACHE_NAME, true);

      HealthStatus healthStatus = clusterHealth.getHealthStatus();

      assertEquals(HealthStatus.HEALTHY, healthStatus);
   }

   public void testUnhealthyStatusWhenUserCacheIsStopped() throws Exception {
      Cache testCache = cacheManager.getCache(CACHE_NAME, true);
      testCache.stop();

      HealthStatus healthStatus = clusterHealth.getHealthStatus();

      assertEquals(HealthStatus.UNHEALTHY, healthStatus);
   }

   public void testRebalancingStatusWhenUserCacheIsRebalancing() throws Exception {
      Cache mockedCache = mock(Cache.class);
      AdvancedCache mockedAdvancedCache = mock(AdvancedCache.class);
      DistributionManager mockedDistributionManager = mock(DistributionManager.class);
      when(mockedCacheManager.getCacheNames()).thenReturn(Collections.singleton(CACHE_NAME));

      mockRehashInProgress(CACHE_NAME, mockedCache, mockedAdvancedCache, mockedDistributionManager);

      ClusterHealth clusterHealth = new ClusterHealthImpl(mockedCacheManager);

      assertEquals(HealthStatus.REBALANCING, clusterHealth.getHealthStatus());
   }

   public void testHealthyStatusForInternalCaches() throws Exception {
      cacheManager.getCache(INTERNAL_CACHE_NAME, true);

      assertEquals(HealthStatus.HEALTHY, clusterHealth.getHealthStatus());
   }

   public void testUnhealthyStatusWhenInternalCacheIsStopped() throws Exception {
      Cache internalCache = cacheManager.getCache(INTERNAL_CACHE_NAME, true);
      internalCache.stop();

      assertEquals(HealthStatus.UNHEALTHY, clusterHealth.getHealthStatus());
   }

   public void testRebalancingStatusWhenInternalCacheIsRebalancing() throws Exception {
      Cache mockedCache = mock(Cache.class);
      AdvancedCache mockedAdvancedCache = mock(AdvancedCache.class);
      DistributionManager mockedDistributionManager = mock(DistributionManager.class);

      when(mockedCacheManager.getCacheNames()).thenReturn(Collections.emptySet());
      mockRehashInProgress(INTERNAL_CACHE_NAME, mockedCache, mockedAdvancedCache, mockedDistributionManager);

      ClusterHealth clusterHealth = new ClusterHealthImpl(mockedCacheManager);

      assertEquals(HealthStatus.REBALANCING, clusterHealth.getHealthStatus());
   }

   public void testGetNodeNames() throws Exception {
      assertEquals(cacheManager.getAddress().toString(), clusterHealth.getNodeNames().get(0));
   }

   public void testGetNumberOfNodes() throws Exception {
      assertEquals(1, clusterHealth.getNumberOfNodes());
   }

   public void testGetNumberOfNodesWithNullTransport() throws Exception {
      ClusterHealth clusterHealth = new ClusterHealthImpl(mockedCacheManager);

      assertEquals(0, clusterHealth.getNumberOfNodes());
   }

   public void testGetNodeNamesWithNullTransport() throws Exception {
      ClusterHealth clusterHealth = new ClusterHealthImpl(mockedCacheManager);

      assertTrue(clusterHealth.getNodeNames().isEmpty());
   }

   private void mockRehashInProgress(String cacheName, Cache mockedCache, AdvancedCache mockedAdvancedCache, DistributionManager mockedDistributionManager) {
      when(mockedCacheManager.getCache(cacheName, false)).thenReturn(mockedCache);
      when(mockedCache.getAdvancedCache()).thenReturn(mockedAdvancedCache);
      when(mockedCache.getStatus()).thenReturn(INSTANTIATED);
      when(mockedAdvancedCache.getDistributionManager()).thenReturn(mockedDistributionManager);
      when(mockedDistributionManager.isRehashInProgress()).thenReturn(true);
   }

}
