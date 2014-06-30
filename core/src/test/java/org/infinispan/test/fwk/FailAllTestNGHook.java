package org.infinispan.test.fwk;

import org.testng.IHookCallBack;
import org.testng.IHookable;
import org.testng.ITestResult;
import org.testng.SkipException;

/**
 * TestNG hook to fail all tests.
 *
 * Useful to check that the cache managers are shut down properly for failed tests.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class FailAllTestNGHook implements IHookable {
   @Override
   public void run(IHookCallBack iHookCallBack, ITestResult iTestResult) {
      iHookCallBack.runTestMethod(iTestResult);
      throw new SkipException("Induced failure");
   }
}
