package org.infinispan.test.testng;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.IClass;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dpospisi@redhat.com
 * @author Mircea.Markus@jboss.com
 */
public class UnitTestTestNGListener implements ITestListener {

   /**
    * Holds test classes actually running in all threads.
    */
   private ThreadLocal<IClass> threadTestClass = new ThreadLocal<IClass>();
   private static final Log log = LogFactory.getLog(UnitTestTestNGListener.class);

   private AtomicInteger failed = new AtomicInteger(0);
   private AtomicInteger succeeded = new AtomicInteger(0);
   private AtomicInteger skipped = new AtomicInteger(0);

   public void onTestStart(ITestResult res) {
      log.info("Starting test " + getTestDesc(res));
      threadTestClass.set(res.getTestClass());
   }

   synchronized public void onTestSuccess(ITestResult arg0) {
      System.out.println(getThreadId() + " Test " + getTestDesc(arg0) + " succeeded.");
      log.info("Test succeeded " + getTestDesc(arg0) + ".");
      succeeded.incrementAndGet();
      printStatus();
   }

   synchronized public void onTestFailure(ITestResult arg0) {
      System.out.println(getThreadId() + " Test " + getTestDesc(arg0) + " failed.");
      if (arg0.getThrowable() != null) log.error("Test failed " + getTestDesc(arg0), arg0.getThrowable());
      failed.incrementAndGet();
      printStatus();
   }

   synchronized public void onTestSkipped(ITestResult arg0) {
      System.out.println(getThreadId() + " Test " + getTestDesc(arg0) + " skipped.");
      log.info(" Test " + getTestDesc(arg0) + " skipped.");
      if (arg0.getThrowable() != null) log.error("Test skipped : " + arg0.getThrowable(), arg0.getThrowable());
      skipped.incrementAndGet();
      printStatus();
   }

   public void onTestFailedButWithinSuccessPercentage(ITestResult arg0) {
   }

   public void onStart(ITestContext arg0) {
   }

   public void onFinish(ITestContext arg0) {
   }

   private String getThreadId() {
      return "[" + Thread.currentThread().getName() + "]";
   }

   private String getTestDesc(ITestResult res) {
      return res.getMethod().getMethodName() + "(" + res.getTestClass().getName() + ")";
   }

   private void printStatus() {
      System.out.println("Test suite progress: tests succeeded: " + succeeded.get() + ", failed: " + failed.get() + ", skipped: " + skipped.get() + ".");
   }
}
