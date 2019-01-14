package org.infinispan.commons.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.logging.Logger;
import org.testng.IConfigurationListener2;
import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.TestNGException;
import org.testng.annotations.Test;

/**
 * Logs TestNG test progress.
 */
public class TestNGTestListener implements ITestListener, IConfigurationListener2, ISuiteListener {
   private static final Set<String> REQUIRED_GROUPS = new HashSet<>(
      Arrays.asList("unit", "functional", "xsite", "arquillian", "stress", "profiling", "manual", "unstable"));
   private static final Set<String> ALLOWED_GROUPS = new HashSet<>(
      Arrays.asList("unit", "functional", "xsite", "arquillian", "stress", "profiling", "manual", "unstable",
                    "smoke", "java10", "transaction"));
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
      Thread.currentThread().setName("before-" + context.getName());
   }

   @Override
   public void onFinish(ITestContext context) {
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
      List<String> errors = new ArrayList<>();
      Set<Class> classes = new HashSet<>();
      checkAnnotations(errors, classes, suite.getExcludedMethods());
      checkAnnotations(errors, classes, suite.getAllMethods());
      if (!errors.isEmpty()) {
         throw new TestNGException(String.join("\n", errors));
      }
   }

   @Override
   public void onFinish(ISuite suite) {
   }

   private void checkAnnotations(List<String> errors, Set<Class> classes, Collection<ITestNGMethod> methods) {
      for (ITestNGMethod m : methods) {
         checkMethodAnnotations(errors, m);
         checkClassAnnotations(errors, classes, m);
      }
   }

   private void checkMethodAnnotations(List<String> errors, ITestNGMethod m) {
      if (!m.getEnabled())
         return;

      boolean hasRequiredGroup = false;
      for (String g : m.getGroups()) {
         if (!ALLOWED_GROUPS.contains(g)) {
            errors.add(
               "Method " + m.getConstructorOrMethod() +
               " and/or its class has a @Test annotation with the wrong group: " +
               g + ".\nAllowed groups are " + ALLOWED_GROUPS);
            return;
         }
         if (REQUIRED_GROUPS.contains(g)) {
            hasRequiredGroup = true;
         }
      }
      if (!hasRequiredGroup) {
         errors.add("Method " + m.getConstructorOrMethod() + " and/or its class don't have any required group." +
                    "\nRequired groups are " + REQUIRED_GROUPS);
         return;
      }

      // Some stress tests extend a functional test and change some parameters (e.g. number of operations)
      // If the author forgets to override the test methods, they inherit the group from the base class
      Class<?> testClass = m.getRealClass();
      Class<?> declaringClass = m.getConstructorOrMethod().getDeclaringClass();
      Test annotation = testClass.getAnnotation(Test.class);
      if (testClass != declaringClass) {
         if (!Arrays.equals(annotation.groups(), m.getGroups())) {
            errors.add("Method " + m.getConstructorOrMethod() + " was inherited from class " + declaringClass +
                       " with groups " + Arrays.toString(m.getGroups()) +
                       ", but the test class has groups " + Arrays.toString(annotation.groups()));
         }
      }
   }

   private void checkClassAnnotations(List<String> errors, Set<Class> processedClasses, ITestNGMethod m) {
      // Class of the test instance
      Class<?> testClass = m.getTestClass().getRealClass();

      // Check that the test instance's class matches the XmlTest's support class
      // They can be different if a test inherits a @Factory method and doesn't override it
      // Ignore multiple classes in the same XmlTest, which can happen when running from the IDE
      // or when the testName is incorrect (handled below)
      if (m.getXmlTest().getXmlClasses().size() == 1) {
         Class xmlClass = m.getXmlTest().getXmlClasses().get(0).getSupportClass();
         if (xmlClass != testClass && processedClasses.add(xmlClass)) {
            errors.add("Class " + xmlClass.getName() + " must override the @Factory method from base class " +
                       testClass.getName());
            return;
         }
      }

      if (!processedClasses.add(testClass))
         return;

      // Check that the testName in the @Test annotation is set and matches the test class
      // All tests with the same testName or without a testName run in the same XmlTest
      // which means they also run in the same thread, not in parallel
      Test annotation = testClass.getAnnotation(Test.class);
      if (annotation == null || annotation.testName().isEmpty()) {
         errors.add("Class " + testClass.getName() + " does not have a testName");
         return;
      }
      if (!annotation.testName().contains(testClass.getSimpleName())) {
         errors.add("Class " + testClass.getName() + " has an invalid testName: " + annotation.testName());
      }
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
      // beforeConfiguration didn't run, no need to unregister thread
      if (testResult.getThrowable() != null) {
         progressLogger.testIgnored(testName(testResult));
      }
   }
}
