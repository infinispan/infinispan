package org.infinispan.commons.test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ISuiteResult;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.Test;
import org.testng.collections.Maps;
import org.testng.internal.IResultListener2;
import org.testng.internal.Utils;

/**
 * A JUnit XML report generator for Polarion based on the JUnitXMLReporter
 *
 * @author <a href='mailto:afield[at]redhat[dot]com'>Alan Field</a>
 */
public class PolarionJUnitXMLReporter implements IResultListener2, ISuiteListener {
   public static final Pattern DUPLICATE_TESTS_MODULE_PATTERN = Pattern.compile(".*-(embedded|remote|v\\d+)");
   /**
    * keep lists of all the results
    */
   private AtomicInteger m_numFailed = new AtomicInteger(0);
   private AtomicInteger m_numSkipped = new AtomicInteger(0);
   private Map<String, List<ITestResult>> m_allTests = Collections.synchronizedMap(new TreeMap<>());

   /**
    * @see org.testng.IConfigurationListener2#beforeConfiguration(ITestResult)
    */
   @Override
   public void beforeConfiguration(ITestResult tr) {
   }

   /**
    * @see org.testng.ITestListener#onTestStart(ITestResult)
    */
   @Override
   public void onTestStart(ITestResult result) {
   }

   /**
    * @see org.testng.ITestListener#onTestSuccess(ITestResult)
    */
   @Override
   public void onTestSuccess(ITestResult tr) {
      checkDuplicatesAndAdd(tr);
   }

   /**
    * @see org.testng.ITestListener#onTestFailure(ITestResult)
    */
   @Override
   public void onTestFailure(ITestResult tr) {
      checkDuplicatesAndAdd(tr);
      m_numFailed.incrementAndGet();
   }

   /**
    * @see org.testng.ITestListener#onTestFailedButWithinSuccessPercentage(ITestResult)
    */
   @Override
   public void onTestFailedButWithinSuccessPercentage(ITestResult tr) {
      checkDuplicatesAndAdd(tr);
      m_numFailed.incrementAndGet();
   }

   /**
    * @see org.testng.ITestListener#onTestSkipped(ITestResult)
    */
   @Override
   public void onTestSkipped(ITestResult tr) {
      checkDuplicatesAndAdd(tr);
      m_numSkipped.incrementAndGet();
   }

   /**
    * @see org.testng.ITestListener#onStart(ITestContext)
    */
   @Override
   public void onStart(ITestContext context) {
   }

   /**
    * @see org.testng.ITestListener#onFinish(ITestContext)
    */
   @Override
   public void onFinish(ITestContext context) {
   }

   /**
    * @see org.testng.ISuiteListener#onStart(ISuite)
    */
   @Override
   public void onStart(ISuite suite) {
      resetAll();
   }

   /**
    * @see org.testng.ISuiteListener#onFinish(ISuite)
    */
   @Override
   public void onFinish(ISuite suite) {
      generateReport(suite);
   }

   /**
    * @see org.testng.IConfigurationListener#onConfigurationFailure(org.testng.ITestResult)
    */
   @Override
   public void onConfigurationFailure(ITestResult tr) {
      checkDuplicatesAndAdd(tr);
      m_numFailed.incrementAndGet();
   }

   /**
    * @see org.testng.IConfigurationListener#onConfigurationSkip(org.testng.ITestResult)
    */
   @Override
   public void onConfigurationSkip(ITestResult tr) {
   }

   /**
    * @see org.testng.IConfigurationListener#onConfigurationSuccess(org.testng.ITestResult)
    */
   @Override
   public void onConfigurationSuccess(ITestResult itr) {
   }

   /**
    * generate the XML report given what we know from all the test results
    */
   private void generateReport(ISuite suite) {
      // Get elapsed time for testsuite element
      long elapsedTime = 0;
      long testCount = 0;
      for (List<ITestResult> testResults : m_allTests.values()) {
         for (ITestResult tr : testResults) {
            elapsedTime += (tr.getEndMillis() - tr.getStartMillis());
            //            if (tr.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class) != null) {
            //               testCount += tr.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class)
            //                     .invocationCount();
            //            } else {
            testCount++;
            //            }
         }
      }

      PolarionJUnitXMLWriter writer = null;
      try {
         String outputDir = suite.getOutputDirectory().replaceAll(".Surefire suite", "");
         String baseName = String.format("TEST-%s.xml", getSuiteName(suite));
         File outputFile = new File(outputDir, baseName);
         writer = new PolarionJUnitXMLWriter(outputFile);
         writer.start(getModuleSuffix(), testCount, m_numSkipped.get(), m_numFailed.get(), elapsedTime, true);

         // TODO Also write a report based on suite.getAllInvokedMethod() and check for differences
         writeTestResults(writer, m_allTests.values());
      } catch (Exception e) {
         System.err.println("Error writing test report");
         e.printStackTrace(System.err);
      } finally {
         try {
            if (writer != null) {
               writer.close();
            }
         } catch (Exception e) {
            System.err.println("Error writing test report");
            e.printStackTrace(System.err);
         }
      }
   }

   private void writeTestResults(PolarionJUnitXMLWriter document, Collection<List<ITestResult>> results)
      throws XMLStreamException {
      synchronized (results) {
         for (List<ITestResult> testResults : results) {
            boolean hasFailures = false;
            // A single test method might have multiple invocations
            for (ITestResult tr : testResults) {
               if (!tr.isSuccess()) {
                  hasFailures = true;
                  // Report all failures
                  writeTestCase(document, tr);
               }
            }
            if (!hasFailures) {
               // If there were no failures, report a single success
               writeTestCase(document, testResults.get(0));
            }
         }
      }
   }

   private void writeTestCase(PolarionJUnitXMLWriter writer, ITestResult tr) throws XMLStreamException {
      String className = tr.getTestClass().getRealClass().getName();
      String testName = testName(tr);
      long elapsedTimeMillis = tr.getEndMillis() - tr.getStartMillis();
      PolarionJUnitXMLWriter.Status status = translateStatus(tr);
      Throwable throwable = tr.getThrowable();
      if (throwable != null) {
         writer.writeTestCase(testName, className, elapsedTimeMillis, status, Utils.shortStackTrace(throwable, true),
                              throwable.getClass().getName(), throwable.getMessage());
      } else {
         writer.writeTestCase(testName, className, elapsedTimeMillis, status, null, null, null);
      }
   }

   private PolarionJUnitXMLWriter.Status translateStatus(ITestResult tr) {
      switch (tr.getStatus()) {
         case ITestResult.FAILURE:
            return PolarionJUnitXMLWriter.Status.FAILURE;
         case ITestResult.SUCCESS:
            return PolarionJUnitXMLWriter.Status.SUCCESS;
         case ITestResult.SUCCESS_PERCENTAGE_FAILURE:
         case ITestResult.SKIP:
            return PolarionJUnitXMLWriter.Status.SKIPPED;
         default:
            return PolarionJUnitXMLWriter.Status.ERROR;
      }
   }

   /**
    * Reset all member variables for next test.
    */
   private void resetAll() {
      m_allTests = Collections.synchronizedMap(Maps.newHashMap());
      m_numFailed.set(0);
      m_numSkipped.set(0);
   }

   private String getSuiteName(ISuite suite) {
      String name = getModuleSuffix();
      Collection<ISuiteResult> suiteResults = suite.getResults().values();
      if (suiteResults.size() == 1) {
         ITestNGMethod[] testMethods = suiteResults.iterator().next().getTestContext().getAllTestMethods();
         if (testMethods.length > 0) {
            Class<?> testClass = testMethods[0].getConstructorOrMethod().getDeclaringClass();
            // If only one test class executed, then use that as the filename
            String className = testClass.getName();
            // If only one test package executed, then use that as the filename
            String packageName = testClass.getPackage().getName();
            boolean oneTestClass = true;
            boolean oneTestPackage = true;
            for (ITestNGMethod method : testMethods) {
               if (!method.getConstructorOrMethod().getDeclaringClass().getName().equals(className)) {
                  oneTestClass = false;
               }
               if (!method.getConstructorOrMethod().getDeclaringClass().getPackage().getName().equals(packageName)) {
                  oneTestPackage = false;
               }
            }
            if (oneTestClass) {
               name = className;
            } else {
               if (oneTestPackage) {
                  name = packageName;
               }
            }
         } else {
            System.err.println(
                  "[" + this.getClass().getSimpleName() + "] Test suite '" + name + "' results have no test methods");
         }
      }
      return name;
   }

   private String testName(ITestResult res) {
      StringBuilder result = new StringBuilder(res.getMethod().getMethodName());
      if (res.getMethod().getConstructorOrMethod().getMethod().isAnnotationPresent(Test.class)) {
         String dataProviderName = res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class)
               .dataProvider();
         // Add parameters for methods that use a data provider only
         if (res.getParameters().length != 0 && (dataProviderName != null && !dataProviderName.isEmpty())) {
            result.append("(").append(deepToStringParameters(res));
         }
         // Add number of invocations to method name
         if (res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class).invocationCount() > 1) {
            if (result.indexOf("(") == -1) {
               result.append("(");
            } else {
               result.append(", ");
            }
            result.append("invoked ").append(
                  res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class).invocationCount())
                  .append(" times");
         }
      }
      String moduleSuffix = getModuleSuffix();
      if (moduleSuffix.contains("hibernate")) {
         if (result.indexOf("(") == -1) {
            result.append("(");
         } else {
            result.append(", ");
         }

         // module
         Matcher moduleMatcher = DUPLICATE_TESTS_MODULE_PATTERN.matcher(moduleSuffix);
         if (moduleMatcher.matches()) {
            result.append(moduleMatcher.group(1));
         }
      }
      // end
      if (result.indexOf("(") != -1) {
         result.append(")");
      }
      return result.toString();
   }

   private String deepToStringParameters(ITestResult res) {
      Object[] parameters = res.getParameters();
      for (int i=0; i<parameters.length; i++) {
         Object parameter = parameters[i];
         if (parameter != null) {
            if (parameter instanceof Path) {
               parameters[i] = ((Path) parameter).getFileName().toString();
            } else if (parameter.getClass().getSimpleName().contains("$$Lambda$")) {
               res.setStatus(ITestResult.FAILURE);
               res.setThrowable(new IllegalStateException("Cannot identify which test is running. Use NamedLambdas.of static method"));
            }
         }
      }
      return Arrays.deepToString(parameters);
   }

   private String getModuleSuffix() {
      // Remove the "-" from the beginning of the string
      return System.getProperty("infinispan.module-suffix").substring(1);
   }

   private void checkDuplicatesAndAdd(ITestResult tr) {
      // Need fully qualified name to guarantee uniqueness in the results map
      String instanceName = tr.getInstanceName();
      String key = instanceName + "." + testName(tr);
      if (m_allTests.containsKey(key)) {
         if (tr.getMethod().getCurrentInvocationCount() == 1 && tr.isSuccess()) {
            System.err.println("[" + this.getClass().getSimpleName() + "] Test case '" + key
                  + "' already exists in the results");
            tr.setStatus(ITestResult.FAILURE);
            tr.setThrowable(new IllegalStateException("Duplicate test: " + key));
         }

         List<ITestResult> itrList = m_allTests.get(key);
         itrList.add(tr);
         m_allTests.put(key, itrList);
      } else {
         ArrayList<ITestResult> itrList = new ArrayList<>();
         itrList.add(tr);
         m_allTests.put(key, itrList);
      }
   }
}
