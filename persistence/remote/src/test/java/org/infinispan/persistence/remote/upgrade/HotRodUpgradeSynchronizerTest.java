package org.infinispan.persistence.remote.upgrade;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "upgrade.hotrod.HotRodUpgradeSynchronizerTest", groups = "functional")
public class HotRodUpgradeSynchronizerTest extends AbstractInfinispanTest {

   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager sourceContainer;
   private Cache<byte[], byte[]> sourceServerDefaultCache;
   private Cache<byte[], byte[]> sourceServerAltCache;
   private EmbeddedCacheManager targetContainer;
   private Cache<byte[], byte[]> targetServerDefaultCache;
   private Cache<byte[], byte[]> targetServerAltCache;
   private RemoteCacheManager sourceRemoteCacheManager;
   private RemoteCache<String, String> sourceRemoteDefaultCache;
   private RemoteCache<String, String> sourceRemoteAltCache;
   private RemoteCacheManager targetRemoteCacheManager;
   private RemoteCache<String, String> targetRemoteDefaultCache;
   private RemoteCache<String, String> targetRemoteAltCache;

   private static final String ALT_CACHE_NAME = "whatever";
   private static final String OLD_PROTOCOL_VERSION = "2.0";

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      sourceContainer = TestCacheManagerFactory
              .createCacheManager(hotRodCacheConfiguration(serverBuilder));
      sourceServerDefaultCache = sourceContainer.getCache();
      sourceServerAltCache = sourceContainer.getCache(ALT_CACHE_NAME);
      sourceServer = HotRodClientTestingUtil.startHotRodServer(sourceContainer);

      ConfigurationBuilder targetConfigurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      targetConfigurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true)
              .ignoreModifications(true).addServer().host("localhost").port(sourceServer.getPort());

      ConfigurationBuilder builderOldVersion = new ConfigurationBuilder();
      builderOldVersion.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true)
              .ignoreModifications(true).remoteCacheName(ALT_CACHE_NAME).protocolVersion(OLD_PROTOCOL_VERSION).addServer().host("localhost").port(sourceServer.getPort());

      targetContainer = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(targetConfigurationBuilder));
      targetContainer.defineConfiguration(ALT_CACHE_NAME, hotRodCacheConfiguration(builderOldVersion).build());

      targetServerDefaultCache = targetContainer.getCache();
      targetServerAltCache = targetContainer.getCache(ALT_CACHE_NAME);
      targetServer = HotRodClientTestingUtil.startHotRodServer(targetContainer);

      sourceRemoteCacheManager = new RemoteCacheManager("localhost", sourceServer.getPort());
      sourceRemoteCacheManager.start();
      sourceRemoteDefaultCache = sourceRemoteCacheManager.getCache();
      sourceRemoteAltCache = sourceRemoteCacheManager.getCache(ALT_CACHE_NAME);
      targetRemoteCacheManager = new RemoteCacheManager("localhost", targetServer.getPort());
      targetRemoteCacheManager.start();
      targetRemoteDefaultCache = targetRemoteCacheManager.getCache();
      targetRemoteAltCache = targetRemoteCacheManager.getCache(ALT_CACHE_NAME);
   }

   public void testSynchronizationViaIterator() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteDefaultCache.put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }
      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteDefaultCache.get("A"));

      RollingUpgradeManager targetUpgradeManager = targetServerDefaultCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      targetUpgradeManager.synchronizeData("hotrod");
      assertEquals(sourceServerDefaultCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size(), targetServerDefaultCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size());

      targetUpgradeManager.disconnectSource("hotrod");

      MetadataValue<String> metadataValue = targetRemoteDefaultCache.getWithMetadata("B");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());
   }

   public void testSynchronizationViaKeyRecording() throws Exception {
      // Fill the old cluster with data
      for (char ch = 'A'; ch <= 'Z'; ch++) {
         String s = Character.toString(ch);
         sourceRemoteAltCache.put(s, s, 20, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      }
      // Verify access to some of the data from the new cluster
      assertEquals("A", targetRemoteAltCache.get("A"));

      RollingUpgradeManager sourceUpgradeManager = sourceServerAltCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      sourceUpgradeManager.recordKnownGlobalKeyset();
      RollingUpgradeManager targetUpgradeManager = targetServerAltCache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      targetUpgradeManager.synchronizeData("hotrod");
      assertEquals(sourceServerAltCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size() - 1, targetServerAltCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size());

      targetUpgradeManager.disconnectSource("hotrod");

      MetadataValue<String> metadataValue = targetRemoteAltCache.getWithMetadata("A");
      assertEquals(20, metadataValue.getLifespan());
      assertEquals(30, metadataValue.getMaxIdle());

   }


   @BeforeMethod
   public void cleanup() {
      sourceServerDefaultCache.clear();
      sourceServerAltCache.clear();
      targetServerDefaultCache.clear();
      targetServerAltCache.clear();
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManagers(sourceRemoteCacheManager, targetRemoteCacheManager);
      HotRodClientTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetContainer, sourceContainer);
   }

}
