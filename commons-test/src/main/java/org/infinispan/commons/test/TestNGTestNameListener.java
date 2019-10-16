package org.infinispan.commons.test;

import static org.infinispan.commons.test.TestNGNameTranslator.translateTestName;

import java.lang.reflect.Field;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.internal.TestResult;

/**
 *
 * This listener fixes the method name by overwriting the private `TestResult.m_name` field and performs some
 * additional processing to ensure the resulting test name is unique.
 *
 * @author <a href='mailto:afield[at]redhat[dot]com'>Alan Field</a>
 * @author <a href='mailto:dlovison[at]redhat[dot]com'>Diego Lovison</a>
 */
public class TestNGTestNameListener implements ITestListener {

   private static final Field m_name;

   static {
      try {
         m_name = TestResult.class.getDeclaredField("m_name");
         m_name.setAccessible(true);
      } catch (NoSuchFieldException e) {
         throw new IllegalStateException();
      }
   }

   @Override
   public void onTestStart(ITestResult result) {
      try {
         m_name.set(result, translateTestName(result));
      } catch (IllegalAccessException e) {
         result.setStatus(ITestResult.FAILURE);
         result.setThrowable(e);
      }
   }

   @Override
   public void onTestSuccess(ITestResult result) {

   }

   @Override
   public void onTestFailure(ITestResult result) {

   }

   @Override
   public void onTestSkipped(ITestResult result) {

   }

   @Override
   public void onTestFailedButWithinSuccessPercentage(ITestResult result) {

   }

   @Override
   public void onStart(ITestContext context) {

   }

   @Override
   public void onFinish(ITestContext context) {

   }
}
