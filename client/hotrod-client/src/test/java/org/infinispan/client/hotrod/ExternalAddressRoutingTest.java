package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.impl.AddressMapper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.github.terma.javaniotcpproxy.StaticTcpProxyConfig;
import com.github.terma.javaniotcpproxy.TcpProxy;

@Test(groups = "functional", testName = "client.hotrod.ExternalAddressRoutingTest")
public class ExternalAddressRoutingTest extends HitsAwareCacheManagersTest {

   private static final int PROXY_TO_SERVER_1_PORT = 9988;
   private static final int PROXY_TO_SERVER_2_PORT = 9989;

   private static final Map<SocketAddress, SocketAddress> proxyToRealAddresses = new HashMap<>();
   private static final Map<SocketAddress, SocketAddress> realAddressesToProxy = new HashMap<>();

   private static final String DIST_ONE_CACHE_NAME = "dist-one-cache";
   private static final String DIST_TWO_CACHE_NAME = "dist-two-cache";

   HotRodServer server1;
   HotRodServer server2;

   ConfigurationBuilder defaultBuilder;
   ConfigurationBuilder distOneBuilder;
   ConfigurationBuilder distTwoBuilder;

   TcpProxy proxyToServer1;
   TcpProxy proxyToServer2;

   RemoteCacheManager rcm;

   protected ConfigurationBuilder defaultCacheConfigurationBuilder() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      defaultBuilder = defaultCacheConfigurationBuilder();
      distOneBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      distOneBuilder.clustering().hash().numOwners(1).numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory(0));
      distTwoBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      distTwoBuilder.clustering().hash().numOwners(1).numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory(1));

      server1 = addHotRodServer();
      server2 = addHotRodServer();

      blockUntilViewReceived(manager(0).getCache(), 2);
      blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      StaticTcpProxyConfig proxyToServer1Configuration = new StaticTcpProxyConfig(PROXY_TO_SERVER_1_PORT, "localhost", server1.getPort());
      proxyToServer1Configuration.setWorkerCount(10);
      proxyToServer1 = new TcpProxy(proxyToServer1Configuration);
      proxyToServer1.start();

      StaticTcpProxyConfig proxyToServer2Configuration = new StaticTcpProxyConfig(PROXY_TO_SERVER_2_PORT, "localhost", server2.getPort());
      proxyToServer2Configuration.setWorkerCount(10);
      proxyToServer2 = new TcpProxy(proxyToServer2Configuration);
      proxyToServer2.start();


      proxyToRealAddresses.put(new InetSocketAddress("localhost", PROXY_TO_SERVER_1_PORT), new InetSocketAddress(server1.getHost(), server1.getPort()));
      proxyToRealAddresses.put(new InetSocketAddress("localhost", PROXY_TO_SERVER_2_PORT), new InetSocketAddress(server2.getHost(), server2.getPort()));

      realAddressesToProxy.put(new InetSocketAddress(server1.getHost(), server1.getPort()), new InetSocketAddress("localhost", PROXY_TO_SERVER_1_PORT));
      realAddressesToProxy.put(new InetSocketAddress(server2.getHost(), server2.getPort()), new InetSocketAddress("localhost", PROXY_TO_SERVER_2_PORT));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      // We add only one server here. Once the HR client initiates connection, it should have two addresses in the connection pool.
      clientBuilder
            .addServer()
               .host("localhost").port(PROXY_TO_SERVER_2_PORT)
            .addressMapping(LoopbackAddressMapper.class);
      rcm = new RemoteCacheManager(clientBuilder.build());
   }

   @AfterClass
   @Override
   protected void destroy() {
      killServers(server1, server2);
      proxyToServer1.shutdown();
      proxyToServer2.shutdown();
      super.destroy();
   }

   private HotRodServer addHotRodServer() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultBuilder);
      cm.defineConfiguration(DIST_ONE_CACHE_NAME, distOneBuilder.build());
      cm.defineConfiguration(DIST_TWO_CACHE_NAME, distTwoBuilder.build());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm);
      addr2hrServer.put(new InetSocketAddress(server.getHost(), server.getPort()), server);
      return server;
   }

   public void testRoutingWithExternalAddresses() {
      addInterceptors(DIST_ONE_CACHE_NAME);
      addInterceptors(DIST_TWO_CACHE_NAME);

      byte[] keyDistOne = HotRodClientTestingUtil.getKeyForServer(server1, DIST_ONE_CACHE_NAME);
      byte[] keyDistTwo = HotRodClientTestingUtil.getKeyForServer(server2, DIST_TWO_CACHE_NAME);

      assertRequestRouting(keyDistOne, DIST_ONE_CACHE_NAME, server1);
      assertRequestRouting(keyDistTwo, DIST_TWO_CACHE_NAME, server2);
      assertProperServerAddressInTopology(DIST_ONE_CACHE_NAME, PROXY_TO_SERVER_1_PORT);
      assertProperServerAddressInTopology(DIST_TWO_CACHE_NAME, PROXY_TO_SERVER_2_PORT);

   }

   private void assertProperServerAddressInTopology(String cacheName, int serverPort) {
      CacheTopologyInfo cache1Topology = rcm.getTransportFactory().getCacheTopologyInfo(cacheName.getBytes());
      for (SocketAddress serverAddress : cache1Topology.getSegmentsPerServer().keySet()) {
         int port = ((InetSocketAddress) serverAddress).getPort();
         assertEquals(serverPort, port);
      }
   }

   private void assertRequestRouting(byte[] key, String cacheName, HotRodServer server) {
      RemoteCache<Object, Object> rcOne = rcm.getCache(cacheName);
      InetSocketAddress serverAddress = new InetSocketAddress(server.getHost(), server.getPort());
      for (int i = 0; i < 2; i++) {
         log.infof("Routing put test for key %s", Util.printArray(key, false));
         rcOne.put(key, "value");
         assertServerHit(serverAddress, cacheName, i + 1);
      }
   }

   public static class LoopbackAddressMapper implements AddressMapper {

      @Override
      public SocketAddress toExternalAddress(SocketAddress internalAddress) {
         int port = ((InetSocketAddress) internalAddress).getPort();
         if (port == PROXY_TO_SERVER_1_PORT && port == PROXY_TO_SERVER_2_PORT) {
            fail("The Hot Rod client is trying to map a proxy address to the external: " + internalAddress + ". The proxy address IS already an external one. Failing the test");
         }
         return realAddressesToProxy.get(internalAddress);
      }

      @Override
      public SocketAddress toInternalAddress(SocketAddress externalAddress) {
         int port = ((InetSocketAddress) externalAddress).getPort();
         if (port != PROXY_TO_SERVER_1_PORT && port != PROXY_TO_SERVER_2_PORT) {
            fail("The Hot Rod client is trying to map different than proxy address to external: " + externalAddress + ". Failing the test");
         }
         return proxyToRealAddresses.get(externalAddress);
      }
   }

}
