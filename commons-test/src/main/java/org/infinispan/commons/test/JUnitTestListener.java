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

   @Override
   public void testStarted(Description description) throws Exception {
      org.infinispan.commons.test.TestSuiteProgress.testStarted(testName(description));
   }

   @Override
   public void testFinished(Description description) throws Exception {
      org.infinispan.commons.test.TestSuiteProgress.testFinished(testName(description));
   }

   @Override
   public void testFailure(Failure failure) throws Exception {
      org.infinispan.commons.test.TestSuiteProgress.testFailed(testName(failure.getDescription()), failure.getException());
   }

   @Override
   public void testIgnored(Description description) throws Exception {
      org.infinispan.commons.test.TestSuiteProgress.testIgnored(testName(description));
   }

   @Override
   public void testAssumptionFailure(Failure failure) {
      org.infinispan.commons.test.TestSuiteProgress
            .testAssumptionFailed(testName(failure.getDescription()), failure.getException());
   }

   private String testName(Description description) {
      String className = description.isSuite() ? "suite" : description.getTestClass().getSimpleName();
      return className + "." + description.getMethodName();
   }
}
