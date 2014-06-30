package org.infinispan.test.fwk;

import org.testng.IConfigurable;
import org.testng.IConfigureCallBack;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

/**
 * TestNG hook to fail all tests.
 *
 * Useful to check that the cache managers are shut down properly for failed tests.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class FailAllSetupTestNGHook implements IConfigurable {
   @Override
   public void run(IConfigureCallBack callBack, ITestResult testResult) {
      ITestNGMethod testMethod = testResult.getMethod();
      System.out.println("Running " + testMethod.getDescription());

      callBack.runConfigurationMethod(testResult);

      if (testMethod.isBeforeMethodConfiguration() || testMethod.isBeforeClassConfiguration() || testMethod.isBeforeTestConfiguration()) {
         throw new RuntimeException("Induced failure");
      }
   }
}
