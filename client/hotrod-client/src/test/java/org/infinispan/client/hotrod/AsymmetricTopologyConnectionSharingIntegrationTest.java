package org.infinispan.client.hotrod;

import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.transport.netty.OperationChannel;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * End-to-end reproducer for ISPN-18549. Connections are shared by all caches of a {@link RemoteCacheManager}. When a
 * cache stops running on one of the servers, that server sends the client a topology for the cache that no longer lists
 * the node. The client must not close the connection to that node while another cache still runs on it, otherwise the
 * shared connections to healthy nodes are repeatedly closed and reopened.
 */
@Test(testName = "client.hotrod.AsymmetricTopologyConnectionSharingIntegrationTest", groups = "functional")
public class AsymmetricTopologyConnectionSharingIntegrationTest extends MultipleCacheManagersTest {

   private static final String WIDE_CACHE = "wide-cache";
   private static final String NARROW_CACHE = "narrow-cache";

   private final List<Node> nodes = new ArrayList<>();
   private RemoteCacheManager client;

   @Override
   protected void createCacheManagers() {
      // Both caches initially run on every node.
      startNewNode(WIDE_CACHE, NARROW_CACHE);
      startNewNode(WIDE_CACHE, NARROW_CACHE);
      startNewNode(WIDE_CACHE, NARROW_CACHE);
   }

   public void testConnectionToDepartedServerKeptForOtherCaches() {
      Node departingNode = nodes.get(2);

      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServers(getServerList());
      client = new RemoteCacheManager(clientBuilder.build());

      RemoteCache<Integer, String> wide = client.getCache(WIDE_CACHE);
      RemoteCache<Integer, String> narrow = client.getCache(NARROW_CACHE);
      OperationDispatcher dispatcher = Internals.dispatcher(client);

      // Warm up both caches so the client learns they run on all three servers and keeps a connection to each.
      eventually("Client should learn both caches run on all three servers",
            () -> {
               for (int i = 0; i < 20; i++) {
                  wide.put(i, "value");
                  narrow.put(i, "value");
               }
               List<InetSocketAddress> wideServers = dispatcher.getServers(WIDE_CACHE);
               List<InetSocketAddress> narrowServers = dispatcher.getServers(NARROW_CACHE);
               return wideServers != null && wideServers.size() == 3
                     && narrowServers != null && narrowServers.size() == 3;
            });

      // The shared topology address of the departing node, as both caches see it, and the live connection to it.
      InetSocketAddress departingServer = dispatcher.getServers(NARROW_CACHE).stream()
            .filter(address -> address.getPort() == departingNode.getPort())
            .findFirst()
            .orElseThrow(() -> new AssertionError("NARROW_CACHE topology does not contain the departing node"));
      OperationChannel channelBefore = dispatcher.getHandlerForAddress(departingServer);
      assertThat(channelBefore).as("connection to the departing node").isNotNull();
      eventually("connection to the departing node should be established",
            () -> channelBefore.getChannel() != null && channelBefore.getChannel().isActive());

      // NARROW_CACHE stops running on the departing node, while WIDE_CACHE keeps running on it.
      departingNode.cacheManager.getCache(NARROW_CACHE).stop();

      // Exercise NARROW_CACHE until the client learns it no longer runs on the departing node. Operations routed to the
      // departing node before the new topology is learned are expected to fail and are ignored.
      eventually("Client should learn NARROW_CACHE no longer runs on the departing node",
            () -> {
               for (int i = 0; i < 20; i++) {
                  try {
                     narrow.put(i, "value");
                  } catch (Exception ignored) {
                     // operation routed to the departing node, which no longer hosts NARROW_CACHE
                  }
               }
               List<InetSocketAddress> servers = dispatcher.getServers(NARROW_CACHE);
               return servers != null && servers.size() == 2
                     && servers.stream().noneMatch(address -> address.getPort() == departingNode.getPort());
            });

      // The fix: the departing node is still used by WIDE_CACHE, so its shared connection must stay open and be the
      // very same connection instance (it was never closed and reopened).
      OperationChannel channelAfter = dispatcher.getHandlerForAddress(departingServer);
      assertThat(channelAfter)
            .as("connection to the departing node must survive NARROW_CACHE leaving it")
            .isNotNull()
            .isSameAs(channelBefore);
      assertThat(channelAfter.getChannel()).isNotNull();
      assertThat(channelAfter.getChannel().isActive())
            .as("the shared connection to the departing node must still be active")
            .isTrue();

      // WIDE_CACHE keeps working against every server, including the node that left NARROW_CACHE.
      for (int i = 0; i < 20; i++) {
         assertThat(wide.get(i)).isEqualTo("value");
      }
   }

   private String getServerList() {
      return nodes.stream().map(node -> "127.0.0.1:" + node.getPort()).collect(Collectors.joining(";"));
   }

   private void startNewNode(String... caches) {
      Node node = new Node(caches);
      node.start();
      waitForClusterToForm();
      nodes.add(node);
   }

   private org.infinispan.configuration.cache.ConfigurationBuilder cacheConfiguration() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      return builder;
   }

   private final class Node {
      private final String[] cacheNames;
      private HotRodServer server;
      private EmbeddedCacheManager cacheManager;
      private boolean stopped;

      Node(String... cacheNames) {
         this.cacheNames = cacheNames;
      }

      void start() {
         cacheManager = createClusteredCacheManager(hotRodCacheConfiguration(cacheConfiguration()));
         registerCacheManager(cacheManager);
         stream(cacheNames).forEach(cacheName -> {
            cacheManager.defineConfiguration(cacheName, cacheConfiguration().build());
            cacheManager.getCache(cacheName);
         });
         server = startHotRodServer(cacheManager);
      }

      int getPort() {
         return server.getPort();
      }

      void kill() {
         if (!stopped) {
            if (server != null) {
               killServers(server);
            }
            killCacheManagers(cacheManager);
            stopped = true;
         }
      }
   }

   @AfterClass
   @Override
   protected void destroy() {
      HotRodClientTestingUtil.killRemoteCacheManager(client);
      nodes.forEach(Node::kill);
      nodes.clear();
      super.destroy();
   }
}
