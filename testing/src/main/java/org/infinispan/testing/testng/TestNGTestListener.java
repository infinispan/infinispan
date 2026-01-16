package org.infinispan.testing.testng;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.infinispan.testing.BlockHoundHelper;
import org.infinispan.testing.TestSuiteProgress;
import org.infinispan.testing.Testing;
import org.infinispan.testing.ThreadLeakChecker;
import org.jboss.logging.Logger;
import org.testng.IAnnotationTransformer;
import org.testng.IConfigurationListener2;
import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.annotations.ITestAnnotation;
import org.testng.annotations.Test;

/**
 * Logs TestNG test progress.
 */
public class TestNGTestListener implements ITestListener, IConfigurationListener2, ISuiteListener, IAnnotationTransformer {
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
      progressLogger.testIgnored(testName(result));
   }

   @Override
   public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
      progressLogger.testFailed(testName(result), result.getThrowable());
   }

   @Override
   public void onStart(ITestContext context) {
      Thread.currentThread().setName("TestNG-" + context.getName());
      ThreadLeakChecker.testStarted(context.getCurrentXmlTest().getXmlClasses().get(0).getName());
   }

   @Override
   public void onFinish(ITestContext context) {
      String testName = context.getCurrentXmlTest().getXmlClasses().get(0).getName();
      // Mark the test as finished. The actual leak check is in TestNGSuiteChecksTest
      ThreadLeakChecker.testFinished(testName);
   }

   private String testName(ITestResult res) {
      StringBuilder result = new StringBuilder();
      // We prefer using the instance name, in case it's customized,
      // but when running JUnit tests, TestNG sets the instance name to `methodName(className)`
      if (res.getInstanceName().contains(res.getMethod().getMethodName())) {
         result.append(res.getTestClass().getName());
      } else {
         result.append(res.getInstanceName());
      }
      result.append(".").append(res.getMethod().getMethodName());
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
      try {
         Class.forName("reactor.blockhound.BlockHound");
         log.info("BlockHound on classpath, installing non blocking checks!");
         BlockHoundHelper.installBlockHound();
      } catch (ClassNotFoundException  e) {
         log.info("BlockHound not on classpath, not enabling");
      }

      ThreadLeakChecker.saveInitialThreads();
   }

   @Override
   public void onFinish(ISuite suite) {
      ThreadLeakChecker.checkForLeaks("");
   }

   @Override
   public void beforeConfiguration(ITestResult testResult) {
      progressLogger.configurationStarted(testName(testResult));
   }

   @Override
   public void onConfigurationSuccess(ITestResult testResult) {
      progressLogger.configurationFinished(testName(testResult));
   }

   @Override
   public void onConfigurationFailure(ITestResult testResult) {
      if (testResult.getThrowable() != null) {
         progressLogger.configurationFailed(testName(testResult), testResult.getThrowable());
      }
   }

   @Override
   public void onConfigurationSkip(ITestResult testResult) {
      // Unregister thread in case the configuration method threw a SkipException
      if (testResult.getThrowable() != null) {
         progressLogger.testIgnored(testName(testResult));
      }
   }

   @SuppressWarnings("rawtypes")
   @Override
   public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
      Class<?> clazz = testClass != null ? testClass :
                 testMethod != null ? testMethod.getDeclaringClass() :
                 testConstructor != null ? testConstructor.getDeclaringClass() :
                 null;
      if ((clazz.getName().startsWith("org.infinispan.spring.embedded.provider")  ||
      clazz.getName().startsWith("org.infinispan.spring.embedded.support")  ||
      clazz.getName().startsWith("org.infinispan.cdi.embedded.test.util"))
      &&
          annotation.getExpectedExceptions() != null) {
            return; // Skip tests in the Spring Embedded provider that expect exceptions
      }
      if (annotation.getTimeOut() == 0) {
         // Set a default timeout for tests that don't specify one
         log.tracef("Setting timeout for test %s to %d seconds", testMethod, Testing.MAX_TEST_SECONDS);
         annotation.setTimeOut(Testing.MAX_TEST_SECONDS * 1000);
      }
   }
}
