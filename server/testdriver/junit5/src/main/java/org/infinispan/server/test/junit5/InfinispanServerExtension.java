package org.infinispan.server.test.junit5;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.JmxTestClient;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 {@link 'https://junit.org/junit5'} extension. <br/>
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
public class InfinispanServerExtension extends AbstractServerExtension implements
      TestClientDriver,
      BeforeAllCallback,
      BeforeEachCallback,
      AfterEachCallback,
      AfterAllCallback {

   private final TestServer testServer;
   private TestClient testClient;
   public InfinispanServerExtension(InfinispanServerTestConfiguration configuration) {
      testServer = new TestServer(configuration);
   }

   @Override
   public void beforeAll(ExtensionContext extensionContext) {
      initSuiteClasses(extensionContext);
      startTestServer(extensionContext, testServer);
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

   @Override
   public void afterAll(ExtensionContext extensionContext) {
      cleanupSuiteClasses(extensionContext);
      // Only stop the extension resources when all tests in a Suite have been completed
      if (suiteTestClasses.isEmpty() && testServer.isDriverInitialized()) {
         stopTestServer(extensionContext, testServer);
      }
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

   public TestServer getTestServer() {
      return testServer;
   }
   public InfinispanServerDriver getServerDriver() {
      return testServer.getDriver();
   }

   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      return testClient.addScript(remoteCacheManager, script);
   }
}
