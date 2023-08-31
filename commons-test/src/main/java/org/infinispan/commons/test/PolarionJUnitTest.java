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
         case ITestResult.SUCCESS:
            status = failures.isEmpty() ? Status.SUCCESS : Status.FLAKY;
            successes.incrementAndGet();
            break;
         case ITestResult.FAILURE:
            failures.add(tr.getThrowable());
            status = Status.FAILURE;
            break;
         case ITestResult.SKIP:
         case ITestResult.SUCCESS_PERCENTAGE_FAILURE:
            status = Status.SKIPPED;
            break;
         default:
            status = Status.ERROR;
      }
      elapsedTime.addAndGet(tr.getEndMillis() - tr.getStartMillis());
   }

   long elapsedTime() {
      return elapsedTime.get();
   }
}
