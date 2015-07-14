package org.infinispan.test.fwk;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dpospisi@redhat.com
 * @author Mircea.Markus@jboss.com
 */
public class UnitTestTestNGListener implements ITestListener, IInvokedMethodListener, ISuiteListener {

   /**
    * Holds test classes actually running in all threads.
    */
   private ThreadLocal<IClass> threadTestClass = new ThreadLocal<IClass>();
   private static final Log log = LogFactory.getLog(UnitTestTestNGListener.class);

   private AtomicInteger failed = new AtomicInteger(0);
   private AtomicInteger succeeded = new AtomicInteger(0);
   private AtomicInteger skipped = new AtomicInteger(0);
   private AtomicBoolean oomHandled = new AtomicBoolean();

   /**
    * A set containing (pseudo-)unique ids for all the threads that are present in the JVM when the suite is started.
    * TestNG's own threads are not taken into account.
    * The id is generated from thread name, id and hashcode.
    */
   private volatile Set<String> seenThreads;

   public void onTestStart(ITestResult res) {
      log.info("Starting test " + getTestDesc(res));
      addOomLoggingSupport();
      threadTestClass.set(res.getTestClass());
   }

   private void addOomLoggingSupport() {
      final Thread.UncaughtExceptionHandler oldHandler = Thread.getDefaultUncaughtExceptionHandler();
      Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
         public void uncaughtException(final Thread t, final Throwable e) {
            try {
               // we need to ensure we only handle first OOM occurrence (multiple threads could see one) to avoid duplicated thread dumps
               if (e instanceof OutOfMemoryError && oomHandled.compareAndSet(false, true)) {
                  printAllTheThreadsInTheJvm();
               }
            } finally {
               if (oldHandler != null) {
                  // invoke the old handler if any
                  oldHandler.uncaughtException(t, e);
               }
            }
         }
      });
   }

   synchronized public void onTestSuccess(ITestResult arg0) {
      String message = "Test " + getTestDesc(arg0) + " succeeded.";
      System.out.println(getThreadId() + ' ' + message);
      log.info(message);
      succeeded.incrementAndGet();
      printStatus();
   }

   synchronized public void onTestFailure(ITestResult arg0) {
      String message = "Test " + getTestDesc(arg0) + " failed.";
      System.out.println(getThreadId() + ' ' + message);
      log.error(message, arg0.getThrowable());
      failed.incrementAndGet();
      printStatus();
   }

   synchronized public void onTestSkipped(ITestResult arg0) {
      String message = "Test " + getTestDesc(arg0) + " skipped.";
      System.out.println(getThreadId() + ' ' + message);
      log.error(message, arg0.getThrowable());
      skipped.incrementAndGet();
      printStatus();
   }


   public void onTestFailedButWithinSuccessPercentage(ITestResult arg0) {
   }

   public void onStart(ITestContext arg0) {
      String fullName = arg0.getName();
      String simpleName = fullName.substring(fullName.lastIndexOf('.') + 1);
      Class testClass = arg0.getCurrentXmlTest().getXmlClasses().get(0).getSupportClass();
      if (!simpleName.equals(testClass.getSimpleName())) {
         log.warnf("Wrong test name %s for class %s", simpleName, testClass.getSimpleName());
      }
      TestResourceTracker.testStarted(testClass.getName());
   }

   public void onFinish(ITestContext arg0) {
      Class testClass = arg0.getCurrentXmlTest().getXmlClasses().get(0).getSupportClass();
      TestResourceTracker.testFinished(testClass.getName());
   }

   private String getThreadId() {
      return "[" + Thread.currentThread().getName() + "]";
   }

   private String getTestDesc(ITestResult res) {
      return res.getMethod().getMethodName() + "(" + res.getTestClass().getName() + ")";
   }

   private void printStatus() {
      String message = "Test suite progress: tests succeeded: " + succeeded.get() + ", failed: " + failed.get() + ", skipped: " + skipped.get() + ".";
      System.out.println(message);
      log.info(message);
   }

   @Override
   public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
   }

   @Override
   public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
      if (testResult.getThrowable() != null && method.isConfigurationMethod()) {
         String message = String.format("Configuration method %s threw an exception", getTestDesc(testResult));
         System.out.println(message);
         log.error(message, testResult.getThrowable());
      }
   }

   //todo [anistor] this approach can result in more OOM. maybe it's wiser to remove the whole thing and rely on -XX:+HeapDumpOnOutOfMemoryError
   private void printAllTheThreadsInTheJvm() {
      if (log.isTraceEnabled()) {
         Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
         log.tracef("Dumping all %s threads in the system.", allStackTraces.size());
         for (Map.Entry<Thread, StackTraceElement[]> s : allStackTraces.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Thread: ").append(s.getKey().getName()).append(", Stack trace:\n");
            for (StackTraceElement ste: s.getValue()) {
               sb.append("      ").append(ste.toString()).append("\n");
            }
            log.trace(sb.toString());
         }
      }
   }

   @Override
   public void onStart(ISuite isuite) {
      if (log.isTraceEnabled()) {
         // remember the ids of all the threads currently existing in the JVM
         Set<String> seenThreads = new HashSet<String>();
         for (Map.Entry<Thread, StackTraceElement[]> s : Thread.getAllStackTraces().entrySet()) {
            Thread thread = s.getKey();
            if (!thread.getName().startsWith("TestNG")) {
               seenThreads.add(thread.getName() + "-" + thread.getId() + "-" + thread.hashCode());
            }
         }
         this.seenThreads = seenThreads;
      }
   }

   @Override
   public void onFinish(ISuite isuite) {
      log.warn("Possible leaked threads at the end of the test suite:");
      int count = 0;
      for (Map.Entry<Thread, StackTraceElement[]> s : Thread.getAllStackTraces().entrySet()) {
         Thread thread = s.getKey();
         if (thread.getName().startsWith("testng-")
               || seenThreads != null && seenThreads.contains(thread.getName() + "-" + thread.getId() + "-" + thread.hashCode())) {
            continue;
         }
         count++;
         if (log.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Thread: name=").append(thread.getName())
                  .append(", group=").append(thread.getThreadGroup() == null ? null : thread.getThreadGroup().getName())
                  .append(", isDaemon=").append(thread.isDaemon())
                  .append(", isInterrupted=").append(thread.isInterrupted())
                  .append(", Stack trace:\n");
            for (StackTraceElement ste : s.getValue()) {
               sb.append("      ").append(ste.toString()).append("\n");
            }
            log.trace(sb.toString());
         } else {
            log.warnf("Thread Name: %s", thread.getName());
         }
      }
      seenThreads = null;
      log.warnf("Number of threads: %s", count);
   }
}
