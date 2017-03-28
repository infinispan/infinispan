package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;


/**
 * Base test class for single node Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   protected String cacheName = "HotRodCache";
   protected HotRodServer hotRodServer;
   protected HotRodClient hotRodClient;
   protected AdvancedCache<byte[], byte[]> advancedCache;
   private String hotRodJmxDomain = getClass().getSimpleName();

   @Override
   public EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cacheManager = createTestCacheManager();
      advancedCache = cacheManager.<byte[], byte[]>getCache(cacheName).getAdvancedCache();
      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotRodServer = createStartHotRodServer(cacheManager);
      hotRodClient = connectClient();
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName(cacheName),
            hotRodCacheConfiguration());
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      return startHotRodServer(cacheManager);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
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

   protected void shutdownClient() {
      killClient(hotRodClient);
   }

   protected HotRodClient connectClient() {
      return new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName, 60, (byte) 21);
   }
}
