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
   public void testPartialSuiteRerunCompletes() {
      TrackingExtension extension = new TrackingExtension();
      ExtensionContext first = contextFor(RetrySuite.First.class);
      ExtensionContext second = contextFor(RetrySuite.Second.class);

      extension.initSuiteClasses(first);
      assertEquals(Set.of(RetrySuite.First.class, RetrySuite.Second.class), extension.suiteTestClasses);
      extension.cleanupSuiteClasses(first);
      extension.cleanupSuiteClasses(second);
      assertTrue(extension.suiteTestClasses.isEmpty());

      extension.initSuiteClasses(second);
      assertEquals(1, extension.suiteTestClasses.size());
      assertTrue(extension.suiteTestClasses.contains(RetrySuite.Second.class));

      extension.afterAll(second);
      assertEquals(1, extension.completedExecutions.get());
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
   @SelectClasses({RetrySuite.First.class, RetrySuite.Second.class})
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
