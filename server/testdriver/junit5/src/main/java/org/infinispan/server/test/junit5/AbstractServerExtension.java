package org.infinispan.server.test.junit5;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

public class AbstractServerExtension {

   protected final List<Consumer<File>> configurationEnhancers = new ArrayList<>();
   protected final Set<Class<?>> suiteTestClasses = new HashSet<>();

   protected String testName(ExtensionContext extensionContext) {
      // We need to replace the $ for subclasses as it causes issues with the testcontainers docker client
      return extensionContext.getRequiredTestClass().getName().replaceAll("\\$", "-");
   }
   protected void initSuiteClasses(ExtensionContext extensionContext) {
      if (!suiteTestClasses.isEmpty())
         return;

      Class<?> testClass = extensionContext.getRequiredTestClass();
      addSuiteTestClasses(testClass);
      // Add SelectClasses from outer class definition
      addSuiteTestClasses(testClass.getDeclaringClass());
   }

   private void addSuiteTestClasses(Class<?> clazz) {
      if (clazz == null)
         return;

      SelectClasses selectClasses = clazz.getAnnotation(SelectClasses.class);
      if (selectClasses != null)
         Collections.addAll(suiteTestClasses, selectClasses.value());
   }

   protected boolean isSuiteClass(ExtensionContext extensionContext) {
      return extensionContext.getRequiredTestClass().isAnnotationPresent(Suite.class);
   }

   protected void cleanupSuiteClasses(ExtensionContext extensionContext) {
      suiteTestClasses.remove(extensionContext.getRequiredTestClass());
   }

   protected void startTestServer(ExtensionContext extensionContext, TestServer testServer) {
      String testName = testName(extensionContext);
      // Don't manage the server when a test is using the same InfinispanServerExtension instance as the parent suite
      boolean manageServer = !isSuiteClass(extensionContext) && !testServer.isDriverInitialized();
      if (manageServer) {
         testServer.initServerDriver();
         testServer.getDriver().prepare(testName);
         testServer.beforeListeners();
         configurationEnhancers.forEach(c -> c.accept(testServer.getDriver().getConfDir()));
         testServer.getDriver().start(testName);
      }
   }
   protected void startTestClient(ExtensionContext extensionContext, TestClient testClient) {
      // Include getDisplayName to ensure ParameterizedTest uniqueness
      String methodName = String.format("%s.%s.%s", extensionContext.getRequiredTestClass().getSimpleName(), extensionContext.getRequiredTestMethod(), extensionContext.getDisplayName());
      testClient.initResources();
      testClient.setMethodName(methodName);
   }

   protected void stopTestServer(ExtensionContext extensionContext, TestServer testServer) {
      String testName = testName(extensionContext);
      testServer.stopServerDriver(testName);
      testServer.afterListeners();
   }
}
