package org.infinispan.server.test.junit5;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotatedFields;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.ModifierSupport;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

public abstract class AbstractServerExtension implements BeforeAllCallback, AfterAllCallback {

   private static final Log log = LogFactory.getLog(AbstractServerExtension.class);

   protected final List<Consumer<File>> configurationEnhancers = new ArrayList<>();
   protected final Set<Class<?>> suiteTestClasses = new HashSet<>();
   protected Class<?> suite;

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

      if (suite == null && isSuiteClass(extensionContext)) {
         suite = testClass;
      }
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
   public final void beforeAll(ExtensionContext context) throws Exception {
      try {
         initSuiteClasses(context);

         // Inject all classes that are using a static @InfinispanServer(ClusteredIT.class) field.
         Class<?> testClass = context.getRequiredTestClass();
         log.infof("Starting test suite: %s for test %s", suite, testClass);

         injectExtension(testClass, this);

         SelectClasses selectClasses = testClass.getAnnotation(SelectClasses.class);
         if (selectClasses != null) {
            for (Class<?> selectClass : selectClasses.value()) {
               injectExtension(selectClass, this);
            }
         }
         onTestsStart(context);
      } catch (Throwable t) {
         Assertions.fail(String.format("Failed during '%s#beforeAll' suite execution", suite.getName()), t);
      }
   }

   protected abstract void onTestsStart(ExtensionContext extensionContext) throws Exception;

   @Override
   public final void afterAll(ExtensionContext context) {
      try {
         log.infof("Finishing suite: %s for test %s", suite, context.getTestClass().orElse(null));
         cleanupSuiteClasses(context);
         // Only stop the extension resources when all tests in a Suite have been completed
         if (suiteTestClasses.isEmpty()) {
            onTestsComplete(context);
         }
      } catch (Throwable t) {
         Assertions.fail(String.format("Failed during '%s#afterAll' suite execution", suite.getName()), t);
      }
   }

   protected abstract void onTestsComplete(ExtensionContext extensionContext);
}
