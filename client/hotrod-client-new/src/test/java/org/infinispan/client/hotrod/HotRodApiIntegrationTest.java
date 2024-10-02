package org.infinispan.client.hotrod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.api.Infinispan;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncCaches;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.HotRodApiIntegrationTest", groups = {"functional", "smoke"} )
public class HotRodApiIntegrationTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "replSync";
   private SyncCache<String, String> defaultRemote;
   private SyncCache<String, String> remoteCache;
   private Infinispan infinispan;
   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      EmbeddedCacheManager cm = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration());
      cm.defineConfiguration(CACHE_NAME, builder.build());
      cm.getCache(CACHE_NAME);
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      //pass the config file to the cache
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      infinispan = Infinispan.create(clientBuilder.build());

      SyncCaches caches = infinispan.sync().caches();
      remoteCache = caches.get(CACHE_NAME);
      defaultRemote = caches.get("");
   }

   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      infinispan.close();
      infinispan = null;

      HotRodClientTestingUtil.killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testPut() throws Exception {
      CacheEntry<String, String> ce = remoteCache.put("aKey", "aValue");
      assertThat(ce.value()).isNull();
      assertHotRodEquals(cacheManager, CACHE_NAME, "aKey", "aValue");

      ce = defaultRemote.put("otherKey", "otherValue");
      assertThat(ce.value()).isNull();
      assertHotRodEquals(cacheManager, "otherKey", "otherValue");

      assert remoteCache.get("aKey").equals("aValue");
      assert defaultRemote.get("otherKey").equals("otherValue");
   }
}
