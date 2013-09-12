package org.infinispan.persistence.remote;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "persistence.remote.RemoteStoreWrapperTest", groups="functional")
public class RemoteStoreWrapperTest extends AbstractInfinispanTest {

   private HotRodServer sourceServer;
   private HotRodServer targetServer;
   private EmbeddedCacheManager serverCacheManager;
   private Cache<byte[], byte[]> serverCache;
   private EmbeddedCacheManager targetCacheManager;
   private Cache<byte[], byte[]> targetCache;
   private RemoteCacheManager remoteSourceCacheManager;
   private RemoteCache<String, String> remoteSourceCache;
   private RemoteCacheManager remoteTargetCacheManager;
   private RemoteCache<String, String> remoteTargetCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      serverCacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(serverBuilder));
      serverCache = serverCacheManager.getCache();
      sourceServer = TestHelper.startHotRodServer(serverCacheManager);

      remoteSourceCacheManager = TestHelper.getRemoteCacheManager(sourceServer);
      remoteSourceCacheManager.start();
      remoteSourceCache = remoteSourceCacheManager.getCache();

      ConfigurationBuilder clientBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      clientBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class)
         .hotRodWrapping(true)
         .addServer()
            .host(sourceServer.getHost())
            .port(sourceServer.getPort());
      targetCacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration(clientBuilder));
      targetCache = targetCacheManager.getCache();
      targetServer = TestHelper.startHotRodServer(targetCacheManager);

      remoteTargetCacheManager = TestHelper.getRemoteCacheManager(targetServer);
      remoteTargetCacheManager.start();
      remoteTargetCache = remoteTargetCacheManager.getCache();
   }

   public void testEntryWrapping() throws Exception {
      remoteSourceCache.put("k1", "v1");
      remoteSourceCache.put("k2", "v2");
      assertHotRodEquals(serverCacheManager, "k1", "v1");
      assertHotRodEquals(serverCacheManager, "k2", "v2");
      String v1 = remoteTargetCache.get("k1");
      assertEquals("v1", v1);
      String v2 = remoteTargetCache.get("k2");
      assertEquals("v2", v2);
   }

   @BeforeMethod
   public void cleanup() {
      serverCache.clear();
      targetCache.clear();
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManagers(remoteSourceCacheManager, remoteTargetCacheManager);
      HotRodClientTestingUtil.killServers(sourceServer, targetServer);
      TestingUtil.killCacheManagers(targetCacheManager, serverCacheManager);
   }

}
