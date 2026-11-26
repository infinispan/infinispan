package org.infinispan.server.test.jupiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

public class AbstractServerExtensionTest {

   @AfterEach
   public void resetRetryLifecycle() {
      System.clearProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT);
      ServerRetryLifecycle.reset();
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
   public void testSuiteCleanupWaitsForLauncherSessionWhenRetriesAreEnabled() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      InfinispanServerLauncherSessionListener listener = new InfinispanServerLauncherSessionListener();
      LauncherSession session = launcherSession();
      TrackingExtension extension = new TrackingExtension();
      ExtensionContext first = contextFor(RetrySuite.First.class);
      ExtensionContext second = contextFor(RetrySuite.Second.class);
      ExtensionContext third = contextFor(RetrySuite.Third.class);

      listener.launcherSessionOpened(session);
      extension.initSuiteClasses(first);
      extension.afterAll(first);
      extension.initSuiteClasses(second);
      extension.afterAll(second);
      extension.testFailed(third, new AssertionError());
      extension.initSuiteClasses(third);
      extension.afterAll(third);

      assertEquals(0, extension.completedExecutions.get());

      extension.initSuiteClasses(second);
      extension.afterAll(second);
      assertEquals(0, extension.completedExecutions.get());

      listener.launcherSessionClosed(session);
      assertEquals(1, extension.completedExecutions.get());
   }

   @Test
   public void testLauncherSessionsCleanUpOnlyTheirOwnExtensions() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      InfinispanServerLauncherSessionListener listener = new InfinispanServerLauncherSessionListener();
      TrackingExtension firstExtension = new TrackingExtension();
      TrackingExtension secondExtension = new TrackingExtension();
      LauncherSession firstSession = launcherSession();
      LauncherSession secondSession = launcherSession();

      listener.launcherSessionOpened(firstSession);
      completeFailedSuite(firstExtension);
      listener.launcherSessionOpened(secondSession);
      completeFailedSuite(secondExtension);

      listener.launcherSessionClosed(firstSession);
      assertEquals(1, firstExtension.completedExecutions.get());
      assertEquals(0, secondExtension.completedExecutions.get());

      listener.launcherSessionClosed(secondSession);
      assertEquals(1, secondExtension.completedExecutions.get());
   }

   @Test
   public void testLauncherSessionCompletesAllCleanupsWhenOneFails() {
      System.setProperty(ServerRetryLifecycle.RERUN_FAILING_TESTS_COUNT, "1");
      InfinispanServerLauncherSessionListener listener = new InfinispanServerLauncherSessionListener();
      FailingTrackingExtension failingExtension = new FailingTrackingExtension();
      TrackingExtension successfulExtension = new TrackingExtension();
      LauncherSession session = launcherSession();

      listener.launcherSessionOpened(session);
      completeFailedSuite(failingExtension);
      completeFailedSuite(successfulExtension);

      assertThrows(RuntimeException.class, () -> listener.launcherSessionClosed(session));
      assertEquals(1, failingExtension.completedExecutions.get());
      assertEquals(1, successfulExtension.completedExecutions.get());

      listener.launcherSessionClosed(session);
      assertEquals(2, failingExtension.completedExecutions.get());
   }

   @Test
   public void testLauncherSessionListenerIsRegistered() throws IOException {
      try (InputStream stream = getClass().getClassLoader().getResourceAsStream(
            "META-INF/services/org.junit.platform.launcher.LauncherSessionListener")) {
         assertEquals(InfinispanServerLauncherSessionListener.class.getName(), new String(stream.readAllBytes(), StandardCharsets.UTF_8).trim());
      }
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

   private void completeFailedSuite(TrackingExtension extension) {
      ExtensionContext first = contextFor(RetrySuite.First.class);
      ExtensionContext second = contextFor(RetrySuite.Second.class);
      ExtensionContext third = contextFor(RetrySuite.Third.class);
      extension.initSuiteClasses(first);
      extension.afterAll(first);
      extension.initSuiteClasses(second);
      extension.afterAll(second);
      extension.testFailed(third, new AssertionError());
      extension.initSuiteClasses(third);
      extension.afterAll(third);
   }

   private LauncherSession launcherSession() {
      return (LauncherSession) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{LauncherSession.class},
            (proxy, method, args) -> switch (method.getName()) {
               case "equals" -> proxy == args[0];
               case "hashCode" -> System.identityHashCode(proxy);
               default -> throw new UnsupportedOperationException(method.getName());
            });
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
