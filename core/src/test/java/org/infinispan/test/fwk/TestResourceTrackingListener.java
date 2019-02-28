package org.infinispan.test.fwk;

import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

/**
 * TestNG listener to call {@link TestResourceTracker#testStarted(String)} and
 * {@link TestResourceTracker#testFinished(String)} when a test can't extend {@link AbstractInfinispanTest}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class TestResourceTrackingListener implements ITestListener {
   @Override
   public void onTestStart(ITestResult result) {
   }

   @Override
   public void onTestSuccess(ITestResult result) {
   }

   @Override
   public void onTestFailure(ITestResult result) {
   }

   @Override
   public void onTestSkipped(ITestResult result) {
   }

   @Override
   public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
   }

   @Override
   public void onStart(ITestContext context) {
      Class testClass = context.getCurrentXmlTest().getXmlClasses().get(0).getSupportClass();
      TestResourceTracker.testStarted(testClass.getName());
   }

   @Override
   public void onFinish(ITestContext context) {
      Class testClass = context.getCurrentXmlTest().getXmlClasses().get(0).getSupportClass();
      TestResourceTracker.testFinished(testClass.getName());
      ThreadLeakChecker.checkForLeaks(getClass().getName());
   }
}
