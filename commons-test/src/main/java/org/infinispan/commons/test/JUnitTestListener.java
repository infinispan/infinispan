package org.infinispan.commons.test;

import static org.infinispan.commons.test.RunningTestsRegistry.registerThreadWithTest;
import static org.infinispan.commons.test.RunningTestsRegistry.unregisterThreadWithTest;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * Logs JUnit test progress.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class JUnitTestListener extends RunListener {
   private ThreadLocal<Boolean> currentTestIsSuccessful = new ThreadLocal<>();

   private final TestSuiteProgress progressLogger;

   public JUnitTestListener() {
      progressLogger = new TestSuiteProgress();
   }

   @Override
   public void testStarted(Description description) throws Exception {
      String testName = testName(description);
      String simpleName = description.getTestClass().getSimpleName();
      progressLogger.testStarted(testName);
      registerThreadWithTest(testName, simpleName);
      currentTestIsSuccessful.set(true);
   }

   @Override
   public void testFinished(Description description) throws Exception {
      unregisterThreadWithTest();
      if (currentTestIsSuccessful.get()) {
         progressLogger.testSucceeded(testName(description));
      }
   }

   @Override
   public void testFailure(Failure failure) throws Exception {
      currentTestIsSuccessful.set(false);
      progressLogger.testFailed(testName(failure.getDescription()), failure.getException());
   }

   @Override
   public void testIgnored(Description description) throws Exception {
      currentTestIsSuccessful.set(false);
      progressLogger.testIgnored(testName(description));
   }

   @Override
   public void testAssumptionFailure(Failure failure) {
      currentTestIsSuccessful.set(false);
      progressLogger.testAssumptionFailed(testName(failure.getDescription()), failure.getException());
   }

   private String testName(Description description) {
      String className = description.isSuite() ? "suite" : description.getTestClass().getSimpleName();
      return className + "." + description.getMethodName();
   }
}
