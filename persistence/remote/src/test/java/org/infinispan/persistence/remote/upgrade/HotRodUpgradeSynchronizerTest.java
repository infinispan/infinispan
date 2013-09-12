package org.infinispan.persistence.remote.upgrade;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(testName = "upgrade.hotrod.HotRodUpgradeSynchronizerTest", groups = "functional")
public class HotRodUpgradeSynchronizerTest extends AbstractInfinispanTest {

   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<byte[], byte[]> sourceServerCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<byte[], byte[]> targetServerCache;
   private RemoteCacheManager sourceRemoteCacheManager;
   private RemoteCache<String, String> sourceRemoteCache;
   private RemoteCacheManager targetRemoteCacheManager;
   private RemoteCache<String, String> targetRemoteCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      sourceContainer = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(serverBuilder));
      sourceServerCache = sourceContainer.getCache();
      sourceServer = TestHelper.startHotRodServer(sourceContainer);

      ConfigurationBuilder targetConfigurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      targetConfigurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true).addServer().host("localhost").port(sourceServer.getPort());

      targetContainer = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(targetConfigurationBuilder));
      targetServerCache = targetContainer.getCache();
      targetServer = TestHelper.startHotRodServer(targetContainer);

      sourceRemoteCacheManager = new RemoteCacheManager("localhost", sourceServer.getPort());
      sourceRemoteCacheManager.start();
      sourceRemoteCache = sourceRemoteCacheManager.getCache();

      targetRemoteCacheManager = new RemoteCacheManager("localhost", targetServer.getPort());
      targetRemoteCacheManager.start();
      targetRemoteCache = targetRemoteCacheManager.getCache();
   }

   public void testSynchronization() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteCache.put(s, s);
      }
      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteCache.get("A"));

      RollingUpgradeManager sourceUpgradeManager = sourceServerCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      sourceUpgradeManager.recordKnownGlobalKeyset();
      RollingUpgradeManager targetUpgradeManager = targetServerCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      targetUpgradeManager.synchronizeData("hotrod");
      // The server contains one extra key: MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS
      assertEquals(sourceServerCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).size() - 1, targetServerCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).size());

      targetUpgradeManager.disconnectSource("hotrod");
   }

   @BeforeMethod
   public void cleanup() {
      sourceServerCache.clear();
      targetServerCache.clear();
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManagers(sourceRemoteCacheManager, targetRemoteCacheManager);
      HotRodClientTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
