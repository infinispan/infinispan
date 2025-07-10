package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.RemoteStoreMixedAccessTest", groups="functional")
public class RemoteStoreMixedAccessTest extends AbstractInfinispanTest {

   private HotRodServer hrServer;
   private EmbeddedCacheManager serverCacheManager;
   private Cache<String, String> serverCache;
   private EmbeddedCacheManager clientCacheManager;
   private Cache<String, String> clientCache;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<String, String> remoteCache;

   @BeforeClass
   public void setup() throws Exception {
      ConfigurationBuilder serverBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      serverBuilder.memory().maxCount(100)
            .expiration().wakeUpInterval(10L);
      serverCacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(serverBuilder));
      serverCache = serverCacheManager.getCache();
      hrServer = HotRodClientTestingUtil.startHotRodServer(serverCacheManager);

      ConfigurationBuilder clientBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      clientBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class)
         .segmented(false)
         .addServer()
            .host(hrServer.getHost())
            .port(hrServer.getPort())
            .addProperty(RemoteStore.MIGRATION, "true");
      clientCacheManager = TestCacheManagerFactory.createCacheManager(clientBuilder);
      clientCache = clientCacheManager.getCache();

      remoteCacheManager = new RemoteCacheManager(
            HotRodClientTestingUtil.newRemoteConfigurationBuilder(hrServer)
                  .build()
      );
      remoteCacheManager.start();
      remoteCache = remoteCacheManager.getCache();
   }

   public void testMixedAccess() {
      remoteCache.put("k1", "v1");
      String rv1 = remoteCache.get("k1");
      assertEquals("v1", rv1);
      MetadataValue<String> mv1 = remoteCache.getWithMetadata("k1");
      assertEquals("v1", mv1.getValue());
      String cv1 = clientCache.get("k1");
      assertEquals("v1", cv1);
   }

   public void testMixedAccessWithLifespan() {
      remoteCache.put("k1", "v1", 120, TimeUnit.SECONDS);
      MetadataValue<String> mv1 = remoteCache.getWithMetadata("k1");
      assertEquals("v1", mv1.getValue());
      assertEquals(120, mv1.getLifespan());
      String cv1 = clientCache.get("k1");
      assertEquals("v1", cv1);
      InternalCacheEntry ice1 = clientCache.getAdvancedCache().getDataContainer().peek("k1");
      assertEquals(120000, ice1.getLifespan());
   }

   public void testMixedAccessWithLifespanAndMaxIdle() {
      remoteCache.put("k1", "v1", 120, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      MetadataValue<String> mv1 = remoteCache.getWithMetadata("k1");
      assertEquals("v1", mv1.getValue());
      assertEquals(120, mv1.getLifespan());
      assertEquals(30, mv1.getMaxIdle());
      String cv1 = clientCache.get("k1");
      assertEquals("v1", cv1);
      InternalCacheEntry ice1 = clientCache.getAdvancedCache().getDataContainer().peek("k1");
      assertEquals(120000, ice1.getLifespan());
      assertEquals(30000, ice1.getMaxIdle());
   }

   @BeforeMethod
   public void cleanup() {
      serverCache.clear();
      clientCache.clear();
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      HotRodClientTestingUtil.killServers(hrServer);
      hrServer = null;
      TestingUtil.killCacheManagers(clientCacheManager, serverCacheManager);
      clientCacheManager = null;
      serverCacheManager = null;
   }

}
