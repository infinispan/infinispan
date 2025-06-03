package org.infinispan.client.hotrod.near;

import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.InvalidatedFailoverNearCacheBloomTest")
public class InvalidatedFailoverNearCacheBloomTest extends InvalidatedFailoverNearCacheTest {
   @Override
   protected <K, V> AssertsNearCache<K, V> createStickyAssertClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.connectionPool().maxActive(1);

      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(4)
            .nearCacheUseBloomFilter(true);
      return AssertsNearCache.create(cache(0), clientBuilder);
   }

   @Override
   protected void killServerForClient(AssertsNearCache<Integer, String> stickyClient) {
      boolean stoppedAServer = false;
      SocketAddress socketAddress = ((InvalidatedNearRemoteCache) stickyClient.remote).getBloomListenerAddress();
      for (HotRodServer server : servers) {
         int serverPort = server.getAddress().getPort();
         if (serverPort == ((InetSocketAddress) socketAddress).getPort()) {
            HotRodClientTestingUtil.killServers(server);
            TestingUtil.killCacheManagers(server.getCacheManager());
            cacheManagers.remove(server.getCacheManager());
            TestingUtil.blockUntilViewsReceived(50000, false, cacheManagers);
            stoppedAServer = true;
            break;
         }
      }
      assertTrue("Could not find a server that mapped to " + socketAddress, stoppedAServer);
   }

   @Override
   protected boolean isClientListenerAttachedToSameServer(AssertsNearCache<Integer, String> client1, AssertsNearCache<Integer, String> client2) {
      SocketAddress client1Address = ((InvalidatedNearRemoteCache) client1.remote).getBloomListenerAddress();
      SocketAddress client2Address = ((InvalidatedNearRemoteCache) client2.remote).getBloomListenerAddress();
      return client1Address.equals(client2Address);
   }
}
