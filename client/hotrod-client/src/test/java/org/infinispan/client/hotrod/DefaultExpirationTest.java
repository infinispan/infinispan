package org.infinispan.client.hotrod;

import org.testng.annotations.Test;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.*;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test (testName = "client.hotrod.DefaultExpirationTest", groups = "functional" )
public class DefaultExpirationTest extends SingleCacheManagerTest {
   protected RemoteCache<String, String> remoteCache;
   protected RemoteCacheManager remoteCacheManager;
   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      builder.expiration().lifespan(3, TimeUnit.SECONDS).maxIdle(2, TimeUnit.SECONDS);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      //pass the config file to the cache
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());
      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache();
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      return new RemoteCacheManager(config);
   }


   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   @Test
   public void testDefaultExpiration() throws Exception {
      remoteCache.put("Key", "Value");
      InternalCacheEntry entry = assertHotRodEquals(cacheManager, "Key", "Value");
      assertTrue(entry.canExpire());
      assertEquals(3000, entry.getLifespan());
      assertEquals(2000, entry.getMaxIdle());
      Thread.sleep(5000);
      assertFalse(remoteCache.containsKey("Key"));
   }

}
