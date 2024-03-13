package org.infinispan.commons.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.ITestResult;

public class PolarionJUnitTest {

   public enum Status {SUCCESS, FLAKY, FAILURE, ERROR, SKIPPED}

   final String name;
   final String clazz;
   final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
   final AtomicInteger successes =  new AtomicInteger();
   final AtomicLong elapsedTime = new AtomicLong();

   volatile Status status = Status.ERROR;

   public PolarionJUnitTest(String name, String clazz) {
      this(name, clazz, null);
   }

   public PolarionJUnitTest(String name, String clazz, Throwable t) {
      this.name = name;
      this.clazz = clazz;
      if (t != null) {
         this.failures.add(t);
         this.status = Status.FAILURE;
      }
   }

   void add(ITestResult tr) {
      switch (tr.getStatus()) {
         case ITestResult.SUCCESS -> {
            status = failures.isEmpty() || exceptionsOkay(failures) ? Status.SUCCESS : Status.FLAKY;
            successes.incrementAndGet();
         }
         case ITestResult.FAILURE -> {
            failures.add(tr.getThrowable());
            status = Status.FAILURE;
         }
         case ITestResult.SKIP, ITestResult.SUCCESS_PERCENTAGE_FAILURE -> status = Status.SKIPPED;
         default -> status = Status.ERROR;
      }
      elapsedTime.addAndGet(tr.getEndMillis() - tr.getStartMillis());
   }

   long elapsedTime() {
      return elapsedTime.get();
   }

   int numberOfExecutions() {
      return successes.get() + failures.size();
   }

   boolean exceptionsOkay(List<Throwable> throwables) {
      return throwables.stream().allMatch(t -> {
         do {
            if (exceptionOkay(t)) {
               return true;
            }
         } while ((t = t.getCause()) != null);
         return false;
      });
   }

   boolean exceptionOkay(Throwable t) {
      if (t instanceof IllegalStateException) {
         StackTraceElement topElement = t.getStackTrace()[0];
         // MagicKey can cause random failures where the cluster isn't in a good state, just ignore those
         // failures in regards to being flaky
         return topElement.getClassName().contains("MagicKey") && topElement.getMethodName().equals("<init>");
      }
      return false;
   }
}
