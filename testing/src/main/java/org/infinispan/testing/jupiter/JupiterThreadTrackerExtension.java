package org.infinispan.testing.jupiter;

import org.infinispan.testing.TestResourceTracker;
import org.infinispan.testing.ThreadLeakChecker;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class JupiterThreadTrackerExtension implements BeforeAllCallback, AfterAllCallback {

   @Override
   public void afterAll(ExtensionContext context) {
      String testName = context.getDisplayName();
      TestResourceTracker.testFinished(testName);
      ThreadLeakChecker.testFinished(testName);
   }

   @Override
   public void beforeAll(ExtensionContext context) {
      String testName = context.getDisplayName();
      TestResourceTracker.testStarted(testName);
      ThreadLeakChecker.testStarted(testName);
   }
}
