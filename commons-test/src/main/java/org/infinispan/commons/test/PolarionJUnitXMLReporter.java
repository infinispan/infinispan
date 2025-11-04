package org.infinispan.commons.test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.annotations.Test;
import org.testng.internal.IResultListener2;

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
   private static final AtomicInteger m_numFailed = new AtomicInteger(0);
   private static final AtomicInteger m_numSkipped = new AtomicInteger(0);
   private static final Map<String, PolarionJUnitTest> m_allTests = Collections.synchronizedMap(new TreeMap<>());

   private static final int rerunFailingTestsCount = Integer.parseInt(System.getProperty("rerunFailingTestsCount", "0"));

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
   }

   /**
    * @see org.testng.ISuiteListener#onFinish(ISuite)
    */
   @Override
   public void onFinish(ISuite suite) {
      generateReport();
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
   private void generateReport() {
      Map<String, Map<String, List<PolarionJUnitTest>>> testsByClassAndInstance;
      synchronized (m_allTests) {
         // Group tests by class name and then by instance parameters
         testsByClassAndInstance = new TreeMap<>();
         for (Map.Entry<String, PolarionJUnitTest> entry : m_allTests.entrySet()) {
            String key = entry.getKey();
            PolarionJUnitTest test = entry.getValue();

            // Extract instance identifier from the key (format: "instanceName.testName")
            // For factory tests, instanceName contains parameters like "TableJdbcStoreFunctionalTest[H2, transactionalCache=true, ...]"
            String instanceKey = key.substring(0, key.lastIndexOf('.'));

            testsByClassAndInstance
                  .computeIfAbsent(test.clazz, k -> new TreeMap<>())
                  .computeIfAbsent(instanceKey, k -> new ArrayList<>())
                  .add(test);
         }
      }

      String outputDir = String.format("%s/surefire-reports", System.getProperty("build.directory"));
      for (Map.Entry<String, Map<String, List<PolarionJUnitTest>>> classEntry : testsByClassAndInstance.entrySet()) {
         String className = classEntry.getKey();
         Map<String, List<PolarionJUnitTest>> instanceMap = classEntry.getValue();

         // Use index to differentiate multiple instances of the same class
         int instanceIndex = 0;
         for (Map.Entry<String, List<PolarionJUnitTest>> instanceEntry : instanceMap.entrySet()) {
            List<PolarionJUnitTest> instanceTests = instanceEntry.getValue();

            // Calculate statistics per instance
            long instanceTestCount = instanceTests.size();
            long instanceElapsedTime = instanceTests.stream().mapToLong(PolarionJUnitTest::elapsedTime).sum();
            long instanceSkippedCount = instanceTests.stream().filter(t -> t.status == PolarionJUnitTest.Status.SKIPPED).count();
            long instanceFailedCount = instanceTests.stream().filter(t -> t.status == PolarionJUnitTest.Status.FAILURE || t.status == PolarionJUnitTest.Status.ERROR).count();

            // Generate unique filename based on class and instance index
            String fileName = generateUniqueFileName(className, instanceIndex, instanceMap.size());
            File outputFile = new File(outputDir, fileName);

            try (PolarionJUnitXMLWriter writer = new PolarionJUnitXMLWriter(outputFile)){
               writer.start(className, instanceTestCount, instanceSkippedCount, instanceFailedCount, instanceElapsedTime, true);
               for (PolarionJUnitTest testCase : instanceTests)
                  writer.writeTestCase(testCase);
            } catch (Exception e) {
               System.err.printf("Error writing test report '%s'%n", outputFile.getName());
               e.printStackTrace(System.err);
            }

            instanceIndex++;
         }
      }
   }

   /**
    * Generate a unique filename for the test report based on class name and instance index
    */
   private String generateUniqueFileName(String className, int instanceIndex, int totalInstances) {
      // If there's only one instance, use the simple format without index
      if (totalInstances == 1) {
         return String.format("TEST-%s.xml", className);
      }

      // For multiple instances, append the index
      return String.format("TEST-%s-%d.xml", className, instanceIndex);
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
      PolarionJUnitTest meta;
      if (m_allTests.containsKey(key)) {
         meta = m_allTests.get(key);
         if (duplicateTest(tr, meta)) {
            System.err.printf("[%s] Test case '%s' already exists in the results%n",
                  this.getClass().getSimpleName(), key);
            tr.setStatus(ITestResult.FAILURE);
            tr.setThrowable(new IllegalStateException("Duplicate test: " + key));
         }
      } else {
         String testName = testName(tr);
         String className = tr.getTestClass().getRealClass().getName();
         meta = new PolarionJUnitTest(testName, className);
      }
      meta.add(tr);
      m_allTests.put(key, meta);
   }

   // Guard against duplicate test names across test instances whilst supporting TestNG invocationCount
   private boolean duplicateTest(ITestResult tr, PolarionJUnitTest meta) {
      int invocationCount;
      if (tr.getMethod().getConstructorOrMethod().getMethod().isAnnotationPresent(Test.class)) {
         Test test = tr.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class);
         invocationCount = test.invocationCount();
      } else {
         invocationCount = 1;
      }

      int numberOfExecutions = meta.numberOfExecutions();
      return numberOfExecutions > rerunFailingTestsCount && numberOfExecutions > invocationCount;
   }
}
