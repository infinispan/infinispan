package org.infinispan.commons.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
   private Set<Long> startupThreads;
   private boolean suiteRunning;


   public TestNGTestListener() {
      progressLogger = new TestSuiteProgress();
   }

   @Override
   public void onTestStart(ITestResult result) {
      progressLogger.testStarted(testName(result));
   }

   @Override
   public void onTestSuccess(ITestResult result) {
      progressLogger.testFinished(testName(result));
   }

   @Override
   public void onTestFailure(ITestResult result) {
      progressLogger.testFailed(testName(result), result.getThrowable());
   }

   @Override
   public void onTestSkipped(ITestResult result) {
      progressLogger.testIgnored(testName(result));
   }

   @Override
   public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
      progressLogger.testFailed(testName(result), result.getThrowable());
   }

   @Override
   public void onStart(ITestContext context) {
   }

   @Override
   public void onFinish(ITestContext context) {
   }

   private String testName(ITestResult res) {
      StringBuilder result = new StringBuilder();
      result.append(res.getTestClass().getRealClass().getName()).append(".").append(res.getMethod().getMethodName());
      if (res.getMethod().getConstructorOrMethod().getMethod().isAnnotationPresent(Test.class)) {
         String dataProviderName = res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class)
               .dataProvider();
         // Add parameters for methods that use a data provider only
         if (res.getParameters().length != 0 && (dataProviderName != null && !dataProviderName.isEmpty())) {
            result.append("(").append(Arrays.deepToString(res.getParameters())).append(")");
         }
      }
      return result.toString();
   }

   @Override
   public void onStart(ISuite isuite) {
      Set<Long> threads = new HashSet<>();
      for (Map.Entry<Thread, StackTraceElement[]> s : Thread.getAllStackTraces().entrySet()) {
         Thread thread = s.getKey();
         if (!thread.getName().startsWith("TestNG")) {
            threads.add(thread.getId());
         }
      }
      startupThreads = threads;
      suiteRunning = true;
   }

   @Override
   public void onFinish(ISuite isuite) {
      // TestNG invokes this method twice, ignore it the second time
      boolean firstTime = suiteRunning;
      suiteRunning = false;
      if (!firstTime)
         return;

      int count = 0;
      for (Map.Entry<Thread, StackTraceElement[]> s : Thread.getAllStackTraces().entrySet()) {
         Thread thread = s.getKey();
         if (ignoreThread(thread))
            continue;

         if (count == 0) {
            log.warn("Possible leaked threads at the end of the test suite:");
         }
         count++;
         // "management I/O-2" #55 prio=5 os_prio=0 tid=0x00007fe6a8134000 nid=0x7f9d runnable
         // [0x00007fe64e4db000]
         //    java.lang.Thread.State:RUNNABLE
         log.warnf("\"%s\" #%d %sprio=%d tid=0x%x nid=NA %s", thread.getName(), count,
               thread.isDaemon() ? "daemon " : "", thread.getPriority(), thread.getId(),
               thread.getState().toString().toLowerCase());
         log.warnf("   java.lang.Thread.State: %s", thread.getState());
         for (StackTraceElement ste : s.getValue()) {
            log.warnf("\t%s", ste);
         }
      }
   }

   private boolean ignoreThread(Thread thread) {
      String threadName = thread.getName();
      return threadName.startsWith("testng-") || threadName.startsWith("ForkJoinPool.commonPool-worker-") || startupThreads.contains(thread.getId());
   }

   @Override
   public void beforeConfiguration(ITestResult testResult) {
      log.debugf("Before setup %s", testResult.getMethod().getMethodName());
   }

   @Override
   public void onConfigurationSuccess(ITestResult testResult) {
      log.debugf("After setup %s", testResult.getMethod().getMethodName());
   }

   @Override
   public void onConfigurationFailure(ITestResult testResult) {
      if (testResult.getThrowable() != null) {
         progressLogger.setupFailed(testName(testResult), testResult.getThrowable());
      }
   }

   @Override
   public void onConfigurationSkip(ITestResult testResult) {
      if (testResult.getThrowable() != null) {
         progressLogger.testIgnored(testName(testResult));
      }
   }
}
