package org.infinispan.server.hotrod.test;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtils.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtils.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtils.startHotRodServer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.ChannelFuture;

public abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   public static final String cacheName = "HotRodCache";
   protected HotRodServer hotRodServer;
   HotRodClient hotRodClient;
   AdvancedCache<byte[], byte[]> advancedCache;
   private final String hotRodJmxDomain = getClass().getSimpleName();

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cacheManager = createTestCacheManager();
      Cache<byte[], byte[]> cache = cacheManager.getCache(cacheName);
      advancedCache = cache.getAdvancedCache();
      return cacheManager;
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   @Override
   protected void setup() throws Exception {
      super.setup();
      hotRodServer = createStartHotRodServer(cacheManager);
      hotRodClient = connectClient();
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      return startHotRodServer(cacheManager);
   }

   @AfterClass(alwaysRun = true)
   public void destroyAfterClass() {
      log.debug("Test finished, close cache, client and Hot Rod server");
      super.destroyAfterClass();
      shutdownClient();
      killServer(hotRodServer);
   }

   protected HotRodServer server() {
      return hotRodServer;
   }

   protected HotRodClient client() {
      return hotRodClient;
   }

   protected String jmxDomain() {
      return hotRodJmxDomain;
   }

   protected ChannelFuture shutdownClient() {
      return killClient(hotRodClient);
   }

   protected HotRodClient connectClient() {
      return new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName, 60, (byte) 21);
   }
}
