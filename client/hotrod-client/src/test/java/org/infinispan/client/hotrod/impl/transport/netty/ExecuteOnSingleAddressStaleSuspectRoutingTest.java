package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.newRemoteConfigurationBuilder;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketAddress;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.ExecuteOnSingleAddressStaleSuspectRoutingTest")
public class ExecuteOnSingleAddressStaleSuspectRoutingTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      hotrodServer = startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = newRemoteConfigurationBuilder();
      // BASIC intelligence keeps the configured server list fixed and routes through the balancer.
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);
      clientBuilder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      clientBuilder.addServer().host("127.0.0.1").port(hotrodServer.getPort() + 1);
      clientBuilder.addServer().host("127.0.0.1").port(hotrodServer.getPort() + 2);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @AfterClass(alwaysRun = true)
   public void shutDownHotrod() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testStaleSuspectDoesNotDivertFromOnlyReachableServer() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();

      // Warm up so the value exists on the only reachable server and a channel to it is established.
      cache.put("k", "v");
      assertThat(cache.get("k")).isEqualTo("v");

      OperationDispatcher dispatcher = TestingUtil.extractField(remoteCacheManager, "dispatcher");

      // The two configured-but-unreachable servers are already suspected from startup.
      // Add the only live server to the failed list too, e.g., server was restarted or something like that.
      SocketAddress aliveAddress = dispatcher.getServers(HotRodConstants.DEFAULT_CACHE_NAME).stream()
            .filter(address -> address.getPort() == hotrodServer.getPort())
            .findFirst()
            .orElseThrow(() -> new AssertionError("live server address not found in dispatcher server list"));
      dispatcher.getConnectionFailedServers().add(aliveAddress);

      // The operation should still succeed.
      // It might be retried because some of the servers are not live, but it should eventually land on the live node.
      assertThat(cache.get("k")).isEqualTo("v");
   }
}
