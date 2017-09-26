package org.infinispan.commons.test;

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

   private final TestSuiteProgress progressLogger;

   public JUnitTestListener() {
      progressLogger = new TestSuiteProgress();
   }

   @Override
   public void testStarted(Description description) throws Exception {
      progressLogger.testStarted(testName(description));
   }

   @Override
   public void testFinished(Description description) throws Exception {
      progressLogger.testFinished(testName(description));
   }

   @Override
   public void testFailure(Failure failure) throws Exception {
      progressLogger.testFailed(testName(failure.getDescription()), failure.getException());
   }

   @Override
   public void testIgnored(Description description) throws Exception {
      progressLogger.testIgnored(testName(description));
   }

   @Override
   public void testAssumptionFailure(Failure failure) {
      progressLogger.testAssumptionFailed(testName(failure.getDescription()), failure.getException());
   }

   private String testName(Description description) {
      String className = description.isSuite() ? "suite" : description.getTestClass().getSimpleName();
      return className + "." + description.getMethodName();
   }
}
