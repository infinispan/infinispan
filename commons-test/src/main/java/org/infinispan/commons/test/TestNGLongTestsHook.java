package org.infinispan.commons.test;

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
         hookCallBack.runTestMethod(testResult);
   }
}
