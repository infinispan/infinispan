package org.infinispan.testing.jupiter;

import org.infinispan.testing.TestSuiteProgress;
import org.infinispan.testing.ThreadLeakChecker;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class JupiterTestListener implements TestExecutionListener {

   private final TestSuiteProgress progressLogger = new TestSuiteProgress();

   @Override
   public void testPlanExecutionStarted(TestPlan plan) {
      TestSuiteProgress.printTestJDKInformation();
      ThreadLeakChecker.saveInitialThreads();
   }

   @Override
   public void testPlanExecutionFinished(TestPlan plan) {
      try {
         ThreadLeakChecker.checkForLeaks("");
      } catch (Throwable e) {
         progressLogger.configurationFailed("[ERROR]", e);
         throw e;
      }
   }

   @Override
   public void executionStarted(TestIdentifier test) {
      String testName = testName(test);
      progressLogger.testStarted(testName);
   }

   private String testName(TestIdentifier test) {
      return test.getDisplayName();
   }

   @Override
   public void executionFinished(TestIdentifier test, TestExecutionResult result) {
      switch (result.getStatus()) {
         case FAILED:
            progressLogger.testFailed(testName(test), result.getThrowable().get());
            break;
         case ABORTED:
            progressLogger.testIgnored(testName(test));
            break;
         case SUCCESSFUL:
            progressLogger.testSucceeded(testName(test));
      }
   }

   @Override
   public void executionSkipped(TestIdentifier test, String reason) {
      progressLogger.testAssumptionFailed(testName(test), reason);
   }
}
