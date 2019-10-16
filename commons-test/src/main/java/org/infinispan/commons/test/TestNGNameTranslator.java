package org.infinispan.commons.test;

import static org.infinispan.commons.test.ModuleNameTranslator.appendModule;

import java.nio.file.Path;
import java.util.Arrays;

import org.testng.ITestResult;
import org.testng.annotations.Test;

/**
 * When a test is annotated with {@code @Test(testName = "foo")}, Based on JUnitXMLReporter. reports all the test
 * methods in the class as having name "foo", ignoring the data provider parameters and the method name reported
 * by {@code NamedTestMethod} (which includes the AbstractInfinispanTest parameters).
 *
 * Also, when running the same test but with a different module, only one test will be reported. For the JCache module,
 * the same test will be executed in the remote and embedded module. In this case, we add the module name in the test
 * name in order to find out what was the module that failed.
 */
public class TestNGNameTranslator {

   public static String translateTestName(ITestResult res) {
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
      appendModule(result);
      if (result.indexOf("(") != -1) {
         result.append(")");
      }
      return result.toString();
   }

   private static String deepToStringParameters(ITestResult res) {
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
}
