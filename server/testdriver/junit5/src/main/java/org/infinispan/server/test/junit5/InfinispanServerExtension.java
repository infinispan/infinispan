package org.infinispan.server.test.junit5;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;

import java.lang.reflect.Field;
import java.util.function.Predicate;

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
import org.junit.platform.commons.support.ModifierSupport;
import org.junit.platform.suite.api.SelectClasses;

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

   private void injectFields(Class<?> testClass, Object testInstance,
                             Object value, Predicate<Field> predicate) {
      findAnnotatedFields(testClass, InfinispanServer.class, predicate)
            .forEach(field -> {
               try {
                  field.setAccessible(true);
                  field.set(testInstance, value);
               }
               catch (Exception ex) {
                  throw new RuntimeException(ex);
               }
            });
   }

   private void injectExtension(Class<?> testClass, Object value) {
      injectFields(testClass, null, value, ModifierSupport::isStatic);
   }

   @Override
   public void beforeAll(ExtensionContext extensionContext) {
      initSuiteClasses(extensionContext);
      startTestServer(extensionContext, testServer);

      Class<?> testClass = extensionContext.getRequiredTestClass();

      injectExtension(testClass, this);

      SelectClasses selectClasses = testClass.getAnnotation(SelectClasses.class);
      if (selectClasses != null) {
         for (Class<?> selectClass : selectClasses.value()) {
            injectExtension(selectClass, this);
         }
      }
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
      if (suiteTestClasses.isEmpty()) {
         if (testServer.isDriverInitialized())
            stopTestServer(extensionContext, testServer);
         testServer.afterListeners();
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

   @Override
   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      return testClient.addScript(remoteCacheManager, script);
   }

   @Override
   public boolean isServerInContainer() {
      return testServer.getDriver() instanceof  ContainerInfinispanServerDriver;
   }
}
