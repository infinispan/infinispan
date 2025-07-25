package org.infinispan.server.test.junit5;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientXSiteDriver;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * <a href="https://junit.org/junit5">JUnit 5</a> extension. <br/>
 *
 *  * Creates a cluster of sites to be used for running multiple tests for Cross-Site replication functionality
 *
 * {@code
 *
 *    static final InfinispanServerExtensionBuilder LON_SERVER = InfinispanServerExtensionBuilder.config("XSiteServerTest.xml").numServers(NUM_SERVERS);
 *    static final InfinispanServerExtensionBuilder NYC_SERVER = InfinispanServerExtensionBuilder.config("XSiteServerTest.xml").numServers(NUM_SERVERS);
 *
 *    @RegisterExtension
 *    static InfinispanXSiteServerExtension SERVER_TEST = new InfinispanXSiteServerExtensionBuilder()
 *          .addSite(LON, LON_SERVER)
 *          .addSite(NYC, NYC_SERVER)
 *          .build();
 * }
 *
 * @author Gustavo Lira
 * @since 12
 */
public class InfinispanXSiteServerExtension extends AbstractServerExtension implements
      TestClientXSiteDriver,
      BeforeEachCallback,
      AfterEachCallback {

   protected final List<TestServer> testServers;
   protected final Map<String, TestClient> testClients = new HashMap<>();

   public InfinispanXSiteServerExtension(List<TestServer> testServers) {
      this.testServers = testServers;
   }

   @Override
   protected void onTestsStart(ExtensionContext extensionContext) throws Exception {
      testServers.forEach((it) -> startTestServer(extensionContext, it));
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) {
      testServers.forEach((it) -> {
         TestClient testClient = new TestClient(it);
         startTestClient(extensionContext, testClient);
         testClients.put(it.getSiteName(), testClient);
      });
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      testClients.values().forEach(TestClient::clearResources);
   }

   @Override
   protected void onTestsComplete(ExtensionContext extensionContext) {
      testServers.stream()
            .filter(TestServer::isDriverInitialized)
            .forEach(server -> {
               if (server.isDriverInitialized())
                  stopTestServer(extensionContext, server);
               server.afterListeners();
            });
   }

   @Override
   public HotRodTestClientDriver hotrod(String siteName) {
      return testClients.get(siteName).hotrod();
   }

   @Override
   public RestTestClientDriver rest(String siteName) {
      return testClients.get(siteName).rest();
   }

   @Override
   public MemcachedTestClientDriver memcached(String siteName) {
      return testClients.get(siteName).memcached();
   }

   @Override
   //All of methodName will be the same
   public String getMethodName() {
      return testClients.values().iterator().next().getMethodName();
   }

   @Override
   public String hostAndPort(String siteName) {
      for (TestServer server : testServers) {
         if (siteName.equals(server.getSiteName())) {
            String host = server.getDriver().getServerAddress(0).getHostAddress();
            int port = server.getDriver().getServerSocket(0, 11222).getPort();
            return host + ":" + port;
         }
      }
      throw new IllegalStateException("Site " + siteName + " not found.");
   }

   @Override
   public CounterManager getCounterManager(String siteName) {
      return testClients.get(siteName).getCounterManager();
   }

   @Override
   public <K, V> MultimapCacheManager<K, V> getMultimapCacheManager(String siteName) {
      return testClients.get(siteName).getRemoteMultimapCacheManager();
   }

   public List<TestServer> getTestServers() {
      return testServers;
   }
}
