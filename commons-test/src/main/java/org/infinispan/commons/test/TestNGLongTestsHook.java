package org.infinispan.commons.test;

import static org.infinispan.commons.test.RunningTestsRegistry.registerThreadWithTest;
import static org.infinispan.commons.test.RunningTestsRegistry.unregisterThreadWithTest;

import org.testng.IHookCallBack;
import org.testng.IHookable;
import org.testng.ITestResult;

/**
 * TestNG hook to interrupt tests that take too long (usually because of a deadlock).
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class TestNGLongTestsHook implements IHookable {

   @Override
   public void run(IHookCallBack hookCallBack, ITestResult testResult) {
      String testName = testResult.getInstanceName() + "." + testResult.getMethod().getMethodName();
      String simpleName = testResult.getTestClass().getRealClass().getSimpleName();
      registerThreadWithTest(testName, simpleName);
      try {
         hookCallBack.runTestMethod(testResult);
      } finally {
         unregisterThreadWithTest();
      }
   }

}
