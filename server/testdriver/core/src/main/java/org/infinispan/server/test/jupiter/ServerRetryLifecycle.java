package org.infinispan.server.test.jupiter;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Launcher-session resource retained by JUnit only when a failed test is retried.
 */
final class ServerRetryLifecycle implements ExtensionContext.Store.CloseableResource {
   static final String RERUN_FAILING_TESTS_COUNT = "infinispan.server.test.rerunFailingTestsCount";

   private final Set<AbstractServerExtension> failedExtensions =
         java.util.Collections.newSetFromMap(new IdentityHashMap<>());
   private final Set<AbstractServerExtension> successfulRetries =
         java.util.Collections.newSetFromMap(new IdentityHashMap<>());
   private final Map<AbstractServerExtension, ExtensionContext> deferredExtensions = new IdentityHashMap<>();

   void recordFailure(AbstractServerExtension extension) {
      failedExtensions.add(extension);
   }

   void recordSuccess(AbstractServerExtension extension) {
      if (deferredExtensions.containsKey(extension)) {
         successfulRetries.add(extension);
      }
   }

   boolean deferCleanup(AbstractServerExtension extension, ExtensionContext context) {
      if (successfulRetries.remove(extension)) {
         deferredExtensions.remove(extension);
         failedExtensions.remove(extension);
         return false;
      }
      if (failedExtensions.contains(extension) || deferredExtensions.containsKey(extension)) {
         deferredExtensions.put(extension, context);
         return true;
      }
      return false;
   }

   @Override
   public void close() throws Throwable {
      List<Throwable> failures = new ArrayList<>();
      for (Map.Entry<AbstractServerExtension, ExtensionContext> entry : deferredExtensions.entrySet()) {
         try {
            entry.getKey().completeDeferredCleanup(entry.getValue());
         } catch (Throwable t) {
            failures.add(t);
         }
      }
      deferredExtensions.clear();
      failedExtensions.clear();
      successfulRetries.clear();
      if (!failures.isEmpty()) {
         RuntimeException exception = new RuntimeException("Failed to clean up server test resources");
         failures.forEach(exception::addSuppressed);
         throw exception;
      }
   }
}
