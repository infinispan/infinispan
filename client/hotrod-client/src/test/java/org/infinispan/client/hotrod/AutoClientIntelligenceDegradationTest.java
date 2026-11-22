package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies that {@link ClientIntelligence#AUTO} really degrades to {@link ClientIntelligence#BASIC} over Hot Rod when
 * the topology advertised by the server cannot be reached, which is the NAT and container scenario the mode exists
 * for.
 *
 * <p>The server is given a proxy address which is never bound, so the topology it advertises is unreachable while the
 * client's initial server list still works.</p>
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "client.hotrod.AutoClientIntelligenceDegradationTest")
public class AutoClientIntelligenceDegradationTest extends SingleCacheManagerTest {

   /**
    * Port 1 is privileged and never bound, so connecting to it fails immediately rather than timing out.
    */
   private static final int UNREACHABLE_PROXY_PORT = 1;

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // The cache has to be clustered: the server only advertises a topology, and therefore the unreachable proxy
      // address, for clustered caches.
      cacheManager = TestCacheManagerFactory.createClusteredCacheManager(
            hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false)));

      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.proxyHost("127.0.0.1").proxyPort(UNREACHABLE_PROXY_PORT);
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, ServerTestingUtil.findFreePort(), serverBuilder);

      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotrodServer.getPort())
            .clientIntelligence(ClientIntelligence.AUTO);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      hotrodServer = null;
      super.teardown();
   }

   public void testAutoDegradesToBasicAndKeepsWorking() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();

      // The first operation triggers a topology update advertising the unreachable proxy address. Connecting to it
      // fails asynchronously, after which the client falls back to BASIC with its initial server list.
      eventually("The cache should have degraded to BASIC", () -> {
         cache.put(k(), v());
         return ClientIntelligence.BASIC == currentIntelligence(cache.getName());
      });

      // Operations must keep succeeding against the initial server list once degraded
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));

      // AUTO must never reach the wire: the server only knows the values 1 to 3
      assertEquals(ClientIntelligence.BASIC, currentIntelligence(cache.getName()));
   }

   private ClientIntelligence currentIntelligence(String cacheName) {
      return remoteCacheManager.getOperationDispatcher().getClientIntelligence(cacheName);
   }
}
