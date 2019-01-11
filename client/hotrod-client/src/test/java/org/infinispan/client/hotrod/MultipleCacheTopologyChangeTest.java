package org.infinispan.client.hotrod;

import static java.util.Arrays.stream;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


/**
 * @since 9.0
 */
@Test(testName = "client.hotrod.MultipleCacheTopologyChangeTest", groups = "functional")
public class MultipleCacheTopologyChangeTest extends MultipleCacheManagersTest {

   private final List<Node> nodes = new ArrayList<>();
   private RemoteCacheManager client;

   public void testRoundRobinLoadBalancing() throws InterruptedException, IOException {
      String[] caches = new String[]{"cache1", "cache2"};
      Node nodeA = startNewNode(caches);
      Node nodeB = startNewNode(caches);

      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServers(getServerList(nodeA, nodeB));
      client = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<Integer, String> cache1 = client.getCache("cache1");
      RemoteCache<Integer, String> cache2 = client.getCache("cache2");

      startNewNode(caches);

      nodeB.kill();

      assertTrue(cache1.isEmpty());
      assertTrue(cache2.isEmpty());

      nodeA.kill();

      assertTrue(cache1.isEmpty());
      assertTrue(cache2.isEmpty());

   }

   private String getServerList(Node... nodes) {
      return String.join(";", stream(nodes).map(n -> "127.0.0.1:" + n.getPort()).collect(Collectors.toSet()));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
   }

   private Node startNewNode(String... caches) {
      Node node = new Node(caches);
      node.start();
      waitForClusterToForm();
      nodes.add(node);
      return node;
   }

   class Node {
      private final String[] cacheNames;
      HotRodServer server;
      EmbeddedCacheManager cacheManager;
      private boolean stopped;

      public Node(String... cacheNames) {
         this.cacheNames = cacheNames;
      }

      public void start() {
         cacheManager = createClusteredCacheManager(hotRodCacheConfiguration(getConfigurationBuilder()));
         registerCacheManager(cacheManager);
         stream(cacheNames).forEach(cacheName -> {
            cacheManager.defineConfiguration(cacheName, getConfigurationBuilder().build());
            cacheManager.getCache(cacheName);
         });
         server = startHotRodServer(cacheManager);
         waitForClusterToForm(cacheNames);
      }

      public int getPort() {
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

   protected org.infinispan.configuration.cache.ConfigurationBuilder getConfigurationBuilder() {
      org.infinispan.configuration.cache.ConfigurationBuilder c =
              new org.infinispan.configuration.cache.ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      return c;
   }

   @AfterClass
   @Override
   protected void destroy() {
      HotRodClientTestingUtil.killRemoteCacheManager(client);
      nodes.forEach(Node::kill);
   }
}
