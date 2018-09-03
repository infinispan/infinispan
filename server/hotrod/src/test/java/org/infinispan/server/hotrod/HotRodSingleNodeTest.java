package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;


/**
 * Base test class for single node Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   protected static final String cacheName = "HotRodCache";

   protected HotRodServer hotRodServer;
   protected HotRodClient hotRodClient;
   protected AdvancedCache<byte[], byte[]> advancedCache;
   private String hotRodJmxDomain = getClass().getSimpleName();

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cacheManager = createTestCacheManager();
      Cache<byte[], byte[]> cache = cacheManager.getCache(cacheName);
      advancedCache = cache.getAdvancedCache();
      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotRodServer = createStartHotRodServer(cacheManager);
      hotRodClient = connectClient();
   }

   @Override
   protected void teardown() {
      log.debug("Killing Hot Rod client and server");
      killClient(hotRodClient);
      killServer(hotRodServer);
      super.teardown();
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName(cacheName),
            hotRodCacheConfiguration());
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      return startHotRodServer(cacheManager);
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

   protected void shutdownClient() {
      killClient(hotRodClient);
   }

   protected byte protocolVersion() {
      return 21;
   }

   protected HotRodClient connectClient() {
      return new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName, 60, protocolVersion());
   }
}
