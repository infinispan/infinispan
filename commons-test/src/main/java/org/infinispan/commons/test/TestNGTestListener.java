package org.infinispan.commons.test;

import java.util.Arrays;

import org.jboss.logging.Logger;
import org.testng.IConfigurationListener2;
import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.annotations.Test;

/**
 * Logs TestNG test progress.
 */
public class TestNGTestListener implements ITestListener, IConfigurationListener2, ISuiteListener {
   private static final Logger log = Logger.getLogger(TestNGTestListener.class);

   private final TestSuiteProgress progressLogger;

   public TestNGTestListener() {
      progressLogger = new TestSuiteProgress();
   }

   @Override
   public void onTestStart(ITestResult result) {
      progressLogger.testStarted(testName(result));
   }

   @Override
   public void onTestSuccess(ITestResult result) {
      progressLogger.testSucceeded(testName(result));
   }

   @Override
   public void onTestFailure(ITestResult result) {
      progressLogger.testFailed(testName(result), result.getThrowable());
   }

   @Override
   public void onTestSkipped(ITestResult result) {
      // Unregister thread in case the method threw a SkipException
      RunningTestsRegistry.unregisterThreadWithTest();
      progressLogger.testIgnored(testName(result));
   }

   @Override
   public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
      progressLogger.testFailed(testName(result), result.getThrowable());
   }

   @Override
   public void onStart(ITestContext context) {
      Thread.currentThread().setName("testng-" + context.getName());
      ThreadLeakChecker.testStarted(context.getCurrentXmlTest().getXmlClasses().get(0).getName());
   }

   @Override
   public void onFinish(ITestContext context) {
      ThreadLeakChecker.testFinished(context.getCurrentXmlTest().getXmlClasses().get(0).getName());
   }

   private String testName(ITestResult res) {
      StringBuilder result = new StringBuilder();
      result.append(res.getInstanceName()).append(".").append(res.getMethod().getMethodName());
      if (res.getMethod().getConstructorOrMethod().getMethod().isAnnotationPresent(Test.class)) {
         String dataProviderName = res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class)
               .dataProvider();
         // Add parameters for methods that use a data provider only
         if (res.getParameters().length != 0 && !dataProviderName.isEmpty()) {
            result.append("(").append(Arrays.deepToString(res.getParameters())).append(")");
         }
      }
      return result.toString();
   }

   @Override
   public void onStart(ISuite suite) {
      ThreadLeakChecker.saveInitialThreads();
   }

   @Override
   public void onFinish(ISuite suite) {
   }

   @Override
   public void beforeConfiguration(ITestResult testResult) {
      progressLogger.configurationStarted(testName(testResult));
      String simpleName = testResult.getTestClass().getRealClass().getSimpleName();
      RunningTestsRegistry.registerThreadWithTest(testName(testResult), simpleName);
   }

   @Override
   public void onConfigurationSuccess(ITestResult testResult) {
      RunningTestsRegistry.unregisterThreadWithTest();
      progressLogger.configurationFinished(testName(testResult));
   }

   @Override
   public void onConfigurationFailure(ITestResult testResult) {
      RunningTestsRegistry.unregisterThreadWithTest();
      if (testResult.getThrowable() != null) {
         progressLogger.configurationFailed(testName(testResult), testResult.getThrowable());
      }
   }

   @Override
   public void onConfigurationSkip(ITestResult testResult) {
      // Unregister thread in case the configuration method threw a SkipException
      RunningTestsRegistry.unregisterThreadWithTest();
      if (testResult.getThrowable() != null) {
         progressLogger.testIgnored(testName(testResult));
      }
   }
}
