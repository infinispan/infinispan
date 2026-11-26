package org.infinispan.server.test.jupiter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.launcher.LauncherSession;

final class ServerRetryLifecycle {
   static final String RERUN_FAILING_TESTS_COUNT = "infinispan.server.test.rerunFailingTestsCount";

   private static final ThreadLocal<LauncherSession> currentSession = new ThreadLocal<>();
   private static final Map<LauncherSession, Set<AbstractServerExtension>> failedExtensions = new ConcurrentHashMap<>();
   private static final Map<LauncherSession, Map<AbstractServerExtension, ExtensionContext>> deferredExtensions = new ConcurrentHashMap<>();

   private ServerRetryLifecycle() {
   }

   static void sessionOpened(LauncherSession session) {
      currentSession.set(session);
   }

   static void recordFailure(AbstractServerExtension extension) {
      LauncherSession session = currentSession.get();
      if (session != null) {
         failedExtensions.computeIfAbsent(session, ignored -> ConcurrentHashMap.newKeySet()).add(extension);
      }
   }

   static boolean deferCleanup(AbstractServerExtension extension, ExtensionContext context) {
      LauncherSession session = currentSession.get();
      if (session == null || !rerunFailingTestsEnabled()) {
         return false;
      }

      Map<AbstractServerExtension, ExtensionContext> deferred = deferredExtensions.get(session);
      if (failedExtensions.getOrDefault(session, Set.of()).contains(extension) ||
            deferred != null && deferred.containsKey(extension)) {
         deferredExtensions.computeIfAbsent(session, ignored -> new ConcurrentHashMap<>()).put(extension, context);
         return true;
      }
      return false;
   }

   static void sessionClosed(LauncherSession session) {
      try {
         Map<AbstractServerExtension, ExtensionContext> deferred = deferredExtensions.get(session);
         if (deferred != null) {
            List<Throwable> failures = new ArrayList<>();
            deferred.forEach((extension, context) -> {
               try {
                  extension.completeDeferredCleanup(context);
                  deferred.remove(extension, context);
               } catch (Throwable t) {
                  failures.add(t);
               }
            });
            if (deferred.isEmpty()) {
               deferredExtensions.remove(session, deferred);
            }
            if (!failures.isEmpty()) {
               RuntimeException exception = new RuntimeException("Failed to clean up server test resources");
               failures.forEach(exception::addSuppressed);
               throw exception;
            }
         }
      } finally {
         failedExtensions.remove(session);
         if (currentSession.get() == session) {
            currentSession.remove();
         }
      }
   }

   static void reset() {
      currentSession.remove();
      failedExtensions.clear();
      deferredExtensions.clear();
   }

   private static boolean rerunFailingTestsEnabled() {
      return Integer.getInteger(RERUN_FAILING_TESTS_COUNT, 0) > 0;
   }
}
