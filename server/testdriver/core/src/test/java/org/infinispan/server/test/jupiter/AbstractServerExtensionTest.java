package org.infinispan.server.test.jupiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

public class AbstractServerExtensionTest {

   @Test
   public void testSingleClassSuiteRerunCompletes() {
      TrackingExtension extension = new TrackingExtension();
      ExtensionContext second = contextFor(RetrySuite.Second.class);

      completeInitialSuite(extension);

      assertPartialRerunCompletes(extension, second, RetrySuite.Second.class, 2);
   }

   @Test
   public void testTwoClassSuiteRerunCompletes() {
      TrackingExtension extension = new TrackingExtension();
      ExtensionContext second = contextFor(RetrySuite.Second.class);
      ExtensionContext third = contextFor(RetrySuite.Third.class);

      completeInitialSuite(extension);

      extension.initSuiteClasses(second);
      assertEquals(Set.of(RetrySuite.Second.class), extension.suiteTestClasses);
      extension.afterAll(second);
      assertEquals(2, extension.completedExecutions.get());

      extension.initSuiteClasses(third);
      assertEquals(Set.of(RetrySuite.Third.class), extension.suiteTestClasses);
      extension.afterAll(third);
      assertEquals(3, extension.completedExecutions.get());
   }

   @Test
   public void testFullSuiteRerunCompletes() {
      TrackingExtension extension = new TrackingExtension();
      ExtensionContext first = contextFor(RetrySuite.First.class);
      ExtensionContext second = contextFor(RetrySuite.Second.class);
      ExtensionContext third = contextFor(RetrySuite.Third.class);

      completeInitialSuite(extension);

      extension.initSuiteClasses(first);
      assertEquals(Set.of(RetrySuite.First.class), extension.suiteTestClasses);
      extension.afterAll(first);
      assertEquals(2, extension.completedExecutions.get());

      extension.initSuiteClasses(second);
      assertEquals(Set.of(RetrySuite.Second.class), extension.suiteTestClasses);
      extension.afterAll(second);
      assertEquals(3, extension.completedExecutions.get());

      extension.initSuiteClasses(third);
      assertEquals(Set.of(RetrySuite.Third.class), extension.suiteTestClasses);
      extension.afterAll(third);
      assertEquals(4, extension.completedExecutions.get());
   }

   private void completeInitialSuite(TrackingExtension extension) {
      ExtensionContext first = contextFor(RetrySuite.First.class);
      extension.initSuiteClasses(first);
      assertEquals(Set.of(RetrySuite.First.class, RetrySuite.Second.class, RetrySuite.Third.class), extension.suiteTestClasses);
      extension.afterAll(first);
      extension.initSuiteClasses(contextFor(RetrySuite.Second.class));
      extension.afterAll(contextFor(RetrySuite.Second.class));
      extension.initSuiteClasses(contextFor(RetrySuite.Third.class));
      extension.afterAll(contextFor(RetrySuite.Third.class));
      assertTrue(extension.suiteTestClasses.isEmpty());
      assertEquals(1, extension.completedExecutions.get());
   }

   private void assertPartialRerunCompletes(TrackingExtension extension, ExtensionContext context, Class<?> testClass,
                                            int completedExecutions) {
      extension.initSuiteClasses(context);
      assertEquals(Set.of(testClass), extension.suiteTestClasses);

      extension.afterAll(context);
      assertEquals(completedExecutions, extension.completedExecutions.get());
   }

   private ExtensionContext contextFor(Class<?> testClass) {
      return (ExtensionContext) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ExtensionContext.class},
            (proxy, method, args) -> switch (method.getName()) {
               case "getRequiredTestClass" -> testClass;
               case "getTestClass" -> Optional.of(testClass);
               default -> throw new UnsupportedOperationException(method.getName());
            });
   }

   @Suite
   @SelectClasses({RetrySuite.First.class, RetrySuite.Second.class, RetrySuite.Third.class})
   public static class RetrySuite {
      public static class First {
         @Test
         public void test() {
         }
      }

      public static class Second {
         @Test
         public void test() {
         }
      }

      public static class Third {
         @Test
         public void test() {
         }
      }
   }

   static class TrackingExtension extends AbstractServerExtension {
      final AtomicInteger completedExecutions = new AtomicInteger();

      @Override
      protected void onTestsStart(ExtensionContext extensionContext) {
      }

      @Override
      protected void onTestsComplete(ExtensionContext extensionContext) {
         completedExecutions.incrementAndGet();
      }
   }
}
