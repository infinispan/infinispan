package org.infinispan.server.test.jupiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.extension.ExtensionContext;

final class ServerRetryLifecycle {
   static final String RERUN_FAILING_TESTS_COUNT = "infinispan.server.test.rerunFailingTestsCount";

   private static final Map<AbstractServerExtension, ExtensionContext> deferredExtensions = new ConcurrentHashMap<>();

   private ServerRetryLifecycle() {
   }

   static boolean deferCleanup(AbstractServerExtension extension, ExtensionContext context) {
      if (rerunFailingTestsEnabled()) {
         deferredExtensions.put(extension, context);
         return true;
      }
      return false;
   }

   static void completeDeferredCleanups() {
      deferredExtensions.forEach((extension, context) -> {
         if (deferredExtensions.remove(extension, context)) {
            extension.completeDeferredCleanup(context);
         }
      });
   }

   static void reset() {
      deferredExtensions.clear();
   }

   private static boolean rerunFailingTestsEnabled() {
      return Integer.getInteger(RERUN_FAILING_TESTS_COUNT, 0) > 0;
   }
}
