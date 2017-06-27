package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.getServerTopologyId;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

/**
 * Base test class for multi node or clustered Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class HotRodMultiNodeTest extends MultipleCacheManagersTest {
   private List<HotRodServer> hotRodServers = new ArrayList<>();
   private List<HotRodClient> hotRodClients = new ArrayList<>();

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < nodeCount(); i++) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration());
         cacheManagers.add(cm);
      }
      defineCaches(cacheName());
   }

   void defineCaches(String cacheName) {
      cacheManagers.forEach(cm -> cm.defineConfiguration(cacheName, createCacheConfig().build()));
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();

      Integer nextServerPort = serverPort();
      for (int i = 0; i < nodeCount(); i++) {
         hotRodServers.add(startTestHotRodServer(cacheManagers.get(i), nextServerPort));
         nextServerPort += 50;
      }

      hotRodClients = createClients(cacheName());
   }

   protected OptionalInt findHighestPort() {
      return hotRodServers.stream().mapToInt(AbstractProtocolServer::getPort).max();
   }

   List<HotRodClient> createClients(String cacheName) {
      return hotRodServers.stream()
                          .map(s -> createClient(s, cacheName))
                          .collect(Collectors.toList());
   }

   protected HotRodClient createClient(HotRodServer server, String cacheName) {
      return new HotRodClient("127.0.0.1", server.getPort(), cacheName, 60, protocolVersion());
   }

   protected HotRodServer startTestHotRodServer(EmbeddedCacheManager cacheManager, int port) {
      return startHotRodServer(cacheManager, port);
   }

   protected HotRodServer startClusteredServer(int port) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration());
      cacheManagers.add(cm);
      cm.defineConfiguration(cacheName(), createCacheConfig().build());

      HotRodServer newServer;
      try {
         newServer = startHotRodServer(cm, port);
      } catch (Exception e) {
         log.error("Exception starting Hot Rod server", e);
         TestingUtil.killCacheManagers(cm);
         throw e;
      }

      TestingUtil.blockUntilViewsReceived(50000, true, cacheManagers);
      return newServer;
   }

   protected void stopClusteredServer(HotRodServer server) {
      killServer(server);
      TestingUtil.killCacheManagers(server.getCacheManager());
      cacheManagers.remove(server.getCacheManager());
      TestingUtil.blockUntilViewsReceived(50000, false, cacheManagers);
   }

   public int currentServerTopologyId() {
      return getServerTopologyId(servers().get(0).getCacheManager(), cacheName());
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      try {
         log.debug("Test finished, close Hot Rod server");
         hotRodClients.forEach(HotRodTestingUtil::killClient);
         hotRodServers.forEach(ServerTestingUtil::killServer);
      } finally {
         // Stop the caches last so that at stoppage time topology cache can be updated properly
         super.destroy();
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() {
      // Do not clear cache between methods so that topology cache does not get cleared
   }

   protected List<HotRodServer> servers() {
      return hotRodServers;
   }

   protected List<HotRodClient> clients() {
      return hotRodClients;
   }

   protected abstract String cacheName();

   protected abstract ConfigurationBuilder createCacheConfig();

   protected byte protocolVersion() {
      return 21;
   }

   protected int nodeCount() {
      return 2;
   }
}
