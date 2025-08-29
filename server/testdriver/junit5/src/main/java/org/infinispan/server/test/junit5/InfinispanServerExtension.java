package org.infinispan.server.test.junit5;

import java.net.InetAddress;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.JmxTestClient;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.OpenAPITestClientDriver;
import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * <a href="https://junit.org/junit5">JUnit 5</a> extension. <br/>
 *
 * The extension can be used in the most simple way that will work in container mode with a standalone server running.
 *
 * {@code
 *
 *    @RegisterExtension
 *    static InfinispanServerExtension SERVER = InfispanServerExtensionBuilder.server();
 * }
 *
 * {@code
 *
 *    @RegisterExtension
 *    static InfinispanServerExtension SERVER = InfispanServerExtensionBuilder.config("infinispan.xml")
 *          .numServers(1)
 *          .runMode(ServerRunMode.EMBEDDED)
 *          .build();
 * }
 *
 * @author Katia Aresti
 * @since 11
 */
public class InfinispanServerExtension extends AbstractServerExtension implements TestClientDriver, BeforeEachCallback,
      AfterEachCallback {

   private final TestServer testServer;
   private TestClient testClient;
   public InfinispanServerExtension(InfinispanServerTestConfiguration configuration) {
      testServer = new TestServer(configuration);
   }

   @Override
   protected void onTestsStart(ExtensionContext extensionContext) throws Exception {
      startTestServer(extensionContext, testServer);
   }

   @Override
   protected void onTestsComplete(ExtensionContext extensionContext) {
      if (testServer.isDriverInitialized())
         stopTestServer(extensionContext, testServer);
      testServer.afterListeners();
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) {
      this.testClient = new TestClient(testServer);
      startTestClient(extensionContext, testClient);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      testClient.clearResources();
   }

   public void assumeContainerMode() {
      Assumptions.assumeTrue(getServerDriver() instanceof ContainerInfinispanServerDriver, "Requires CONTAINER mode");
   }

   @Override
   public HotRodTestClientDriver hotrod() {
      return testClient.hotrod();
   }

   @Override
   public RestTestClientDriver rest() {
      return testClient.rest();
   }

   @Override
   public OpenAPITestClientDriver openapi() {
      return testClient.openapi();
   }

   @Override
   public RespTestClientDriver resp() {
      return testClient.resp();
   }

   @Override
   public MemcachedTestClientDriver memcached() {
      return testClient.memcached();
   }

   @Override
   public JmxTestClient jmx() {
      return testClient.jmx();
   }

   @Override
   public String getMethodName() {
      return testClient.getMethodName();
   }

   @Override
   public String getMethodName(String qualifier) {
      return testClient.getMethodName(qualifier);
   }

   @Override
   public CounterManager getCounterManager() {
      return testClient.getCounterManager();
   }

   @Override
   public InetAddress getServerAddress(int offset) {
      return getServerDriver().getServerAddress(offset);
   }

   public TestServer getTestServer() {
      return testServer;
   }
   public InfinispanServerDriver getServerDriver() {
      return testServer.getDriver();
   }

   @Override
   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      return testClient.addScript(remoteCacheManager, script);
   }

   @Override
   public boolean isContainerized() {
      return testServer.getDriver() instanceof  ContainerInfinispanServerDriver;
   }
}
