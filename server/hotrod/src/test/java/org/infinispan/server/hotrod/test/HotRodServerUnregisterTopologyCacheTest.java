package org.infinispan.server.hotrod.test;

import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.getDefaultHotRodConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(testName = "server.hotrod.test.HotRodServerUnregisterTopologyCacheTest", groups = "functional")
public class HotRodServerUnregisterTopologyCacheTest extends MultipleCacheManagersTest {

   private static final int CLUSTER_SIZE = 2;

   private List<HotRodServer> hotRodServers;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false), CLUSTER_SIZE);
      startServers();
      waitForClusterToForm();
   }

   private void startServers() {
      hotRodServers = new ArrayList<>(CLUSTER_SIZE);
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         hotRodServers.add(startHotRodServer(manager(i), findFreePort(), getDefaultHotRodConfiguration()));
      }
   }

   public void testServerUnregisterTopologyCacheOnStop() {
      for (HotRodServer server : hotRodServers) {
         String topologyCacheName = server.getConfiguration().topologyCacheName();

         assertTrue(getInternalCacheNames(server).contains(topologyCacheName));

         killServer(server);

         assertFalse(getInternalCacheNames(server).contains(topologyCacheName));
      }
   }

   private Set<String> getInternalCacheNames(HotRodServer server) {
      return server.getCacheManager()
            .getGlobalComponentRegistry()
            .getComponent(InternalCacheRegistry.class)
            .getInternalCacheNames();
   }
}
