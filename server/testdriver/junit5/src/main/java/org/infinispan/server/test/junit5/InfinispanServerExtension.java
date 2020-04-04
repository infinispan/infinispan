package org.infinispan.server.test.junit5;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
public class InfinispanServerExtension implements
      TestClientDriver,
      BeforeAllCallback,
      BeforeTestExecutionCallback,
      AfterTestExecutionCallback,
      AfterAllCallback {

   private final TestServer testServer;
   private final List<Consumer<File>> configurationEnhancers = new ArrayList<>();
   private TestClient testClient;
   private String methodName;

   public InfinispanServerExtension(InfinispanServerTestConfiguration configuration) {
      testServer = new TestServer(configuration);
   }

   @Override
   public void beforeAll(ExtensionContext extensionContext) throws Exception {
      String testName = extensionContext.getRequiredTestClass().getName();
      // Don't manage the server when a test is using the same InfinispanServerRule instance as the parent suite
      boolean manageServer = !testServer.isDriverInitialized();
      if (manageServer) {
         testServer.initServerDriver();
         testServer.beforeListeners();
         testServer.getDriver().prepare(testName);

         configurationEnhancers.forEach(c -> c.accept(testServer.getDriver().getConfDir()));

         testServer.getDriver().start(testName);
      }
   }

   @Override
   public void beforeTestExecution(ExtensionContext extensionContext) throws Exception {
      this.testClient = new TestClient(testServer);
      testClient.initResources();
      methodName =
            extensionContext.getRequiredTestClass().getSimpleName() + "." + extensionContext.getRequiredTestMethod()
                  .getName();
   }

   @Override
   public void afterTestExecution(ExtensionContext extensionContext) throws Exception {
      testClient.clearResources();
   }

   @Override
   public void afterAll(ExtensionContext extensionContext) throws Exception {
      String testName = extensionContext.getRequiredTestClass().getName();
      if (testServer.isDriverInitialized()) {
         testServer.afterListeners();
         testServer.getDriver().stop(testName);
      }
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
   public String getMethodName() {
      return methodName;
   }

   @Override
   public CounterManager getCounterManager() {
      return testClient.getCounterManager();
   }
}
