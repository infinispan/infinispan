package org.infinispan.server.test.jupiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

public class AbstractServerExtensionTest {

   @AfterEach
   public void resetRetryLifecycle() {
      System.clearProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT);
   }

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

   @Test
   public void testSuccessfulRetryCompletesCleanup() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      StoreHolder store = new StoreHolder();
      TrackingExtension extension = new TrackingExtension();
      ExtensionContext first = contextFor(RetrySuite.First.class, store);
      ExtensionContext second = contextFor(RetrySuite.Second.class, store);
      ExtensionContext third = contextFor(RetrySuite.Third.class, store);

      extension.initSuiteClasses(first);
      extension.afterAll(first);
      extension.initSuiteClasses(second);
      extension.afterAll(second);
      extension.testFailed(third, new AssertionError());
      extension.initSuiteClasses(third);
      extension.afterAll(third);

      assertEquals(0, extension.completedExecutions.get());

      extension.initSuiteClasses(second);
      extension.testSuccessful(second);
      extension.afterAll(second);
      assertEquals(1, extension.completedExecutions.get());

      store.close();
      assertEquals(1, extension.completedExecutions.get());
   }

   @Test
   public void testLauncherSessionsCleanUpOnlyTheirOwnExtensions() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      TrackingExtension firstExtension = new TrackingExtension();
      TrackingExtension secondExtension = new TrackingExtension();
      StoreHolder firstStore = new StoreHolder();
      StoreHolder secondStore = new StoreHolder();

      completeFailedSuite(firstExtension, firstStore);
      completeFailedSuite(secondExtension, secondStore);

      firstStore.close();
      assertEquals(1, firstExtension.completedExecutions.get());
      assertEquals(0, secondExtension.completedExecutions.get());

      secondStore.close();
      assertEquals(1, secondExtension.completedExecutions.get());
   }

   @Test
   public void testLauncherSessionCompletesAllCleanupsWhenOneFails() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      FailingTrackingExtension failingExtension = new FailingTrackingExtension();
      TrackingExtension successfulExtension = new TrackingExtension();
      StoreHolder store = new StoreHolder();

      completeFailedSuite(failingExtension, store);
      completeFailedSuite(successfulExtension, store);

      RuntimeException exception = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, store::close);
      assertEquals(1, exception.getSuppressed().length);
      assertEquals(1, failingExtension.completedExecutions.get());
      assertEquals(1, successfulExtension.completedExecutions.get());

      store.close();
      assertEquals(1, failingExtension.completedExecutions.get());
   }

   @Test
   public void testInfinispanServerFieldForwardsFailureNotification() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      StoreHolder store = new StoreHolder();
      TrackingExtension extension = new TrackingExtension();
      InfinispanServerFieldTest.SERVER = extension;
      ExtensionContext first = contextFor(RetrySuite.First.class, store);
      ExtensionContext second = contextFor(RetrySuite.Second.class, store);
      ExtensionContext third = contextFor(RetrySuite.Third.class, store);

      extension.initSuiteClasses(first);
      extension.afterAll(first);
      extension.initSuiteClasses(second);
      extension.afterAll(second);
      new CustomInfinispanExtension().testFailed(contextFor(InfinispanServerFieldTest.class, store), new AssertionError());
      extension.initSuiteClasses(third);
      extension.afterAll(third);

      assertEquals(0, extension.completedExecutions.get());
      store.close();
      assertEquals(1, extension.completedExecutions.get());
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

   private void completeFailedSuite(TrackingExtension extension, StoreHolder store) {
      ExtensionContext first = contextFor(RetrySuite.First.class, store);
      ExtensionContext second = contextFor(RetrySuite.Second.class, store);
      ExtensionContext third = contextFor(RetrySuite.Third.class, store);
      extension.initSuiteClasses(first);
      extension.afterAll(first);
      extension.initSuiteClasses(second);
      extension.afterAll(second);
      extension.testFailed(third, new AssertionError());
      extension.initSuiteClasses(third);
      extension.afterAll(third);
   }

   private ExtensionContext contextFor(Class<?> testClass) {
      return contextFor(testClass, new StoreHolder());
   }

   private ExtensionContext contextFor(Class<?> testClass, StoreHolder store) {
      return (ExtensionContext) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ExtensionContext.class},
            (proxy, method, args) -> switch (method.getName()) {
               case "getRequiredTestClass" -> testClass;
               case "getTestClass" -> Optional.of(testClass);
               case "getStore" -> store.store;
               default -> throw new UnsupportedOperationException(method.getName());
            });
   }

   private static class StoreHolder {
      private final Map<Object, Object> values = new HashMap<>();
      private final ExtensionContext.Store store = (ExtensionContext.Store) Proxy.newProxyInstance(
            AbstractServerExtensionTest.class.getClassLoader(), new Class<?>[]{ExtensionContext.Store.class},
            (proxy, method, args) -> {
               if (method.getName().equals("getOrComputeIfAbsent")) {
                  return values.computeIfAbsent(args[0], ignored -> ((Function<Object, Object>) args[1]).apply(args[0]));
               }
               throw new UnsupportedOperationException(method.getName());
            });

      void close() {
         for (Object value : values.values()) {
            if (value instanceof ExtensionContext.Store.CloseableResource closeable) {
               try {
                  closeable.close();
               } catch (RuntimeException e) {
                  throw e;
               } catch (Throwable t) {
                  throw new RuntimeException(t);
               }
            }
         }
      }
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

   public static class DummySuite extends InfinispanSuite {
   }

   public static class InfinispanServerFieldTest {
      @InfinispanServer(DummySuite.class)
      public static TrackingExtension SERVER;
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

   static class FailingTrackingExtension extends TrackingExtension {
      boolean fail = true;

      @Override
      protected void onTestsComplete(ExtensionContext extensionContext) {
         super.onTestsComplete(extensionContext);
         if (fail) {
            fail = false;
            throw new RuntimeException("Expected cleanup failure");
         }
      }
   }
}
