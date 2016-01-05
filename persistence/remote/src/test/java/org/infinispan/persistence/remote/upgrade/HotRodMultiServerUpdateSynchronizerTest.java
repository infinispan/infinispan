package org.infinispan.persistence.remote.upgrade;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "upgrade.hotrod.HotRodMultiServerUpdateSynchronizerTest", groups = "functional")
public class HotRodMultiServerUpdateSynchronizerTest extends AbstractInfinispanTest {

   private Cluster sourceCluster;
   private Cluster targetCluster;

   @BeforeClass
   public void setup() throws Exception {
      sourceCluster = buildCluster("sourceCluster", 2, null);
      targetCluster = buildCluster("targetCluster", 2, sourceCluster.hotRodServers.get(0).getPort());
   }

   public void testSynchronization() throws Exception {
      RemoteCache<String, String> sourceRemoteCache = sourceCluster.remoteCache;
      RemoteCache<String, String> targetRemoteCache = targetCluster.remoteCache;

      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }

      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteCache.get("A"));

      RollingUpgradeManager upgradeManager = targetCluster.getRollingUpgradeManager();
      long count = upgradeManager.synchronizeData("hotrod");

      assertEquals(26, count);
      assertEquals(sourceCluster.cache.size(), targetCluster.cache.size());

      upgradeManager.disconnectSource("hotrod");

      MetadataValue<String> metadataValue = targetRemoteCache.getWithMetadata("Z");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());

   }

   @AfterClass
   public void tearDown() {
      targetCluster.destroy();
      sourceCluster.destroy();
   }


   private static class Cluster {
      private final RemoteCache<String, String> remoteCache;
      List<HotRodServer> hotRodServers;
      List<EmbeddedCacheManager> embeddedCacheManagers;
      RemoteCacheManager remoteCacheManager;
      private final Cache<String, String> cache;

      public Cluster(List<HotRodServer> hotRodServers, List<EmbeddedCacheManager> embeddedCacheManagers, RemoteCacheManager remoteCacheManager) {
         this.hotRodServers = hotRodServers;
         this.embeddedCacheManagers = embeddedCacheManagers;
         this.remoteCacheManager = remoteCacheManager;
         this.cache = embeddedCacheManagers.get(0).getCache();
         this.remoteCache = remoteCacheManager.getCache();
      }

      private void destroy() {
         embeddedCacheManagers.forEach(TestingUtil::killCacheManagers);
         hotRodServers.forEach(HotRodClientTestingUtil::killServers);
         HotRodClientTestingUtil.killRemoteCacheManagers(remoteCacheManager);
      }

      private RollingUpgradeManager getRollingUpgradeManager() {
         return embeddedCacheManagers.get(0).getCache().getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      }

   }

   private Cluster buildCluster(String name, int numMembers, Integer remoteCachePort) {
      List<HotRodServer> hotRodServers = new ArrayList<>();
      List<EmbeddedCacheManager> embeddedCacheManagers = new ArrayList<>();
      RemoteCacheManager remoteCacheManager;
      for (int i = 0; i < numMembers; i++) {
         GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
         gcb.transport().defaultTransport().clusterName(name);
         ConfigurationBuilder defaultClusteredCacheConfig = AbstractCacheTest.getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
         if (remoteCachePort != null) {
            defaultClusteredCacheConfig.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true)
                    .ignoreModifications(true).addServer().host("localhost").port(remoteCachePort);
         }
         EmbeddedCacheManager clusteredCacheManager = TestCacheManagerFactory.createClusteredCacheManager(gcb, hotRodCacheConfiguration(defaultClusteredCacheConfig));
         embeddedCacheManagers.add(clusteredCacheManager);
         hotRodServers.add(HotRodClientTestingUtil.startHotRodServer(clusteredCacheManager));
      }
      int port = hotRodServers.get(0).getPort();
      Configuration build = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder().addServer().port(port).host("localhost").build();
      remoteCacheManager = new RemoteCacheManager(build);
      return new Cluster(hotRodServers, embeddedCacheManagers, remoteCacheManager);
   }

}
