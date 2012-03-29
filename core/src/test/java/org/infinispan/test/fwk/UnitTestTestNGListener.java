/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.test.fwk;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.IClass;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dpospisi@redhat.com
 * @author Mircea.Markus@jboss.com
 */
public class UnitTestTestNGListener implements ITestListener, IInvokedMethodListener {

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
      addOomLoggingSupport();
      threadTestClass.set(res.getTestClass());
   }

   private void addOomLoggingSupport() {
      Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
         public void uncaughtException(final Thread t, final Throwable e) {
            printAllTheThreadsInTheJvm(e);
         }
      });
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
      String fullName = arg0.getName();
      String simpleName = fullName.substring(fullName.lastIndexOf('.') + 1);
      TestCacheManagerFactory.testStarted(simpleName, fullName);
   }

   public void onFinish(ITestContext arg0) {
      TestCacheManagerFactory.testFinished(arg0.getName());
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

   @Override
   public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
   }

   @Override
   public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
      if (!method.isTestMethod())
         return;
      if (testResult.getThrowable() != null)
         log.errorf(testResult.getThrowable(), "Method %s threw an exception", getTestDesc(testResult));
   }

   public static void printAllTheThreadsInTheJvm(Throwable e) {
      if (e instanceof OutOfMemoryError) {
         Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
         log.tracef("Dumping all %s threads in the system.", allStackTraces.size());
         for (Map.Entry<Thread, StackTraceElement[]> s : allStackTraces.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Thread: ").append(s.getKey().getName()).append(". Stack trace:\n");
            for (StackTraceElement ste: s.getValue()) {
               sb.append("      ").append(ste.toString()).append("\n");
            }
            log.trace(sb.toString());
         }
      }
   }
}
