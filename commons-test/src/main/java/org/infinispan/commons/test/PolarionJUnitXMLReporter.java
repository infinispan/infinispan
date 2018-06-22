package org.infinispan.commons.test;

import java.util.Arrays;

import org.testng.ITestResult;
import org.testng.annotations.Test;
import org.testng.reporters.JUnitReportReporter;

/**
 * A JUnit XML report generator for Polarion based on the JUnitXMLReporter
 *
 * @author <a href='mailto:afield[at]redhat[dot]com'>Alan Field</a>
 * @author <a href='mailto:dlovison[at]redhat[dot]com'>Diego Lovison</a>
 */
public class PolarionJUnitXMLReporter extends JUnitReportReporter {

   @Override
   protected String getTestName(ITestResult res) {
      StringBuilder result = new StringBuilder(res.getMethod().getMethodName());
      if (res.getMethod().getConstructorOrMethod().getMethod().isAnnotationPresent(Test.class)) {
         String dataProviderName = res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class)
               .dataProvider();
         // Add parameters for methods that use a data provider only
         if (res.getParameters().length != 0 && (dataProviderName != null && !dataProviderName.isEmpty())) {
            result.append("(").append(Arrays.deepToString(res.getParameters()));
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
         // JCache tests are a special case
         if (getModuleSuffix().contains("jcache")) {
            if (result.indexOf("(") == -1) {
               result.append("(");
            } else {
               result.append(", ");
            }
            if (getModuleSuffix().contains("infinispan-jcache-remote")) {
               result.append("remote");
            } else {
               result.append("embedded");
            }
         }
         if (result.indexOf("(") != -1) {
            result.append(")");
         }
      }
      return result.toString();
   }

   private String getModuleSuffix() {
      // Remove the "-" from the beginning of the string
      return System.getProperty("infinispan.module-suffix").substring(1);
   }
}
