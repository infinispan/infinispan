package org.infinispan.commons.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

/**
 * Check for leaked threads in the test suite.
 *
 * <p>Every second record new threads and the tests that might have started them.
 * After the last test, log the threads that are still running and the source tests.</p>
 *
 * <p>
 * Strategies for debugging test suite thread leaks:
 * </p>
 * <ul>
 *    <li>Use -Dinfinispan.test.parallel.threads=3 (or even less) to narrow down source tests</li>
 *    <li>Set a conditional breakpoint in Thread.start with the name of the leaked thread</li>
 *    <li>If the thread has the pattern of a particular component, set a conditional breakpoint in that component</li>
 * </ul>
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class ThreadLeakChecker {
   private static final Pattern IGNORED_THREADS_REGEX =
      Pattern.compile("(testng-" +
                      // RunningTestRegistry uses a scheduled executor
                      "|RunningTestsRegistry-Worker" +
                      // TestingUtil.orTimeout
                      "|test-timeout-thread" +
                      // JUnit FailOnTimeout rule
                      "|Time-limited test" +
                      // Distributed streams use ForkJoinPool.commonPool
                      "|ForkJoinPool.commonPool-" +
                      // RxJava
                      "|RxCachedWorkerPoolEvictor" +
                      "|RxSchedulerPurge" +
                      "|globalEventExecutor" +
                      // Narayana
                      "|Transaction Reaper" +
                      // H2
                      "|Generate Seed" +
                      // JDK HTTP client
                      "|Keep-Alive-Timer" +
                      // JVM debug agent thread (sometimes started dynamically by Byteman)
                      "|Attach Listener" +
                      // Hibernate search sometimes leaks consumer threads (ISPN-9890)
                      "|Hibernate Search sync consumer thread for index" +
                      // Reader thread sometimes stays alive for 20s after stop (JGRP-2328)
                      "|NioConnection.Reader" +
                      // java.lang.ProcessHandleImpl
                      "|process reaper" +
                      // Arquillian uses the static default XNIO worker
                      "|XNIO-1 " +
                      // org.apache.mina.transport.socket.nio.NioDatagramAcceptor.DEFAULT_RECYCLER
                      "|ExpiringMapExpirer" +
                      // jboss-modules
                      "|Reference Reaper" +
                      // jboss-remoting
                      "|remoting-jmx client" +
                      // wildfly-controller-client
                      "|management-client-thread" +
                      // IBM JRE specific
                      "|ClassCache Reaper" +
                      // Elytron
                      "|SecurityDomain ThreadGroup" +
                      // Testcontainers
                      "|ducttape" +
                      "|testcontainers" +
                      "|Okio Watchdog" +
                      "|OkHttp ConnectionPool" +
                       // OkHttp uses daemon threads for HTTP/2
                      "|OkHttp Http2Connection" +
                      // The mysql driver uses a daemon thread to check for connection leaks
                      "|mysql-cj-abandoned-connection-cleanup" +
                      ").*");
   private static final String ARQUILLIAN_CONSOLE_CONSUMER =
      "org.jboss.as.arquillian.container.CommonManagedDeployableContainer$ConsoleConsumer";
   private static final boolean ENABLED =
      "true".equalsIgnoreCase(System.getProperty("infinispan.test.checkThreadLeaks", "true"));

   private static final Logger log = Logger.getLogger(ThreadLeakChecker.class);
   private static final Map<Thread, LeakInfo> runningThreads = new ConcurrentHashMap<>();
   private static final Lock lock = new ReentrantLock();

   private static final LeakException IGNORED = new LeakException("IGNORED");
   private static final LeakException UNKNOWN = new LeakException("UNKNOWN");
   private static final ThreadInfoLocal threadInfo = new ThreadInfoLocal();

   private static class ThreadInfoLocal extends InheritableThreadLocal<LeakException> {
      @Override
      protected LeakException childValue(LeakException parentValue) {
         return new LeakException(Thread.currentThread().getName(), parentValue);
      }
   }

   /**
    * A test class has started, and we should consider it as a potential owner for new threads.
    */
   public static void testStarted(String testName) {
      threadInfo.set(new LeakException(testName));
   }

   /**
    * Save the system threads in order to ignore them
    */
   public static void saveInitialThreads() {
      lock.lock();
      try {
         Set<Thread> currentThreads = getThreadsSnapshot();
         for (Thread thread : currentThreads) {
            LeakInfo leakInfo = new LeakInfo(thread, IGNORED);
            runningThreads.putIfAbsent(thread, leakInfo);
         }
      } finally {
         lock.unlock();
      }

      // Initialize the thread-local, in case some tests don't call testStarted()
      threadInfo.set(UNKNOWN);
   }

   /**
    * A test class has finished, and we should not consider it a potential owner for new threads any more.
    */
   public static void testFinished(String testName) {
      threadInfo.set(new LeakException("after-" + testName));
   }

   private static void updateThreadOwnership(String testName) {
      // Update the thread ownership information
      Set<Thread> currentThreads = getThreadsSnapshot();
      runningThreads.keySet().retainAll(currentThreads);

      Field threadLocalsField;
      Method getEntryMethod;
      Field valueField;
      try {
         threadLocalsField = Thread.class.getDeclaredField("inheritableThreadLocals");
         threadLocalsField.setAccessible(true);
         getEntryMethod = Class.forName("java.lang.ThreadLocal$ThreadLocalMap").getDeclaredMethod("getEntry", ThreadLocal.class);
         getEntryMethod.setAccessible(true);
         valueField = Class.forName("java.lang.ThreadLocal$ThreadLocalMap$Entry").getDeclaredField("value");
         valueField.setAccessible(true);
      } catch (NoSuchFieldException | NoSuchMethodException | ClassNotFoundException e) {
         log.error("Error obtaining thread local accessors, ignoring thread leaks");
         return;
      }

      for (Thread thread : currentThreads) {
         if (runningThreads.containsKey(thread))
            continue;

         try {
            Object threadLocalsMap = threadLocalsField.get(thread);
            Object entry = threadLocalsMap != null ? getEntryMethod.invoke(threadLocalsMap, threadInfo) : null;
            LeakException stacktrace = entry != null ? (LeakException) valueField.get(entry) : new LeakException(testName);
            runningThreads.putIfAbsent(thread, new LeakInfo(thread, stacktrace));
         } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Error extracting backtrace of leaked thread " + thread.getName());
         }
      }
   }

   /**
    * Check for leaked threads.
    *
    * Assumes that no tests are running.
    */
   public static void checkForLeaks(String lastTestName) {
      if (!ENABLED)
         return;

      lock.lock();
      try {
         performCheck(lastTestName);
      } finally {
         lock.unlock();
      }
   }

   private static void performCheck(String lastTestName) {
      String ownerTest = "UNKNOWN[" + lastTestName + "]";
      updateThreadOwnership(ownerTest);
      List<LeakInfo> leaks = computeLeaks();

      if (!leaks.isEmpty()) {
         // Give the threads some more time to finish, in case the stop method didn't wait
         try {
            Thread.sleep(1500);
         } catch (InterruptedException e) {
            // Ignore
            Thread.currentThread().interrupt();
         }
         // Update the thread ownership information
         updateThreadOwnership(ownerTest);
         leaks = computeLeaks();
      }

      if (!leaks.isEmpty()) {
         try {
            File reportsDir = new File("target/surefire-reports");
            if (!reportsDir.exists() && !reportsDir.mkdirs()) {
               throw new IOException("Cannot create report directory " + reportsDir.getAbsolutePath());
            }
            PolarionJUnitXMLWriter writer = new PolarionJUnitXMLWriter(
               new File(reportsDir, "TEST-ThreadLeakChecker" + lastTestName + ".xml"));
            String property = System.getProperty("infinispan.modulesuffix");
            String moduleName = property != null ? property.substring(1) : "";
            writer.start(moduleName, leaks.size(), 0, leaks.size(), 0, false);

            for (LeakInfo leakInfo : leaks) {
               String testName = "ThreadLeakChecker";
               Throwable cause = leakInfo.stacktrace;
               while (cause != null) {
                  testName = cause.getMessage();
                  cause = cause.getCause();
               }
               LeakException exception = new LeakException("Leaked thread: " + leakInfo.thread.getName(),
                                                           leakInfo.stacktrace);
               exception.setStackTrace(leakInfo.thread.getStackTrace());

               TestSuiteProgress.fakeTestFailure(testName + ".ThreadLeakChecker", exception);

               StringWriter exceptionWriter = new StringWriter();
               exception.printStackTrace(new PrintWriter(exceptionWriter));
               writer.writeTestCase("ThreadLeakChecker", testName, 0, PolarionJUnitXMLWriter.Status.FAILURE,
                                    exceptionWriter.toString(), exception.getClass().getName(), exception.getMessage());

               leakInfo.markReported();
            }

            writer.close();
         } catch (Exception e) {
            throw new RuntimeException("Error reporting thread leaks", e);
         }
      }
   }

   private static List<LeakInfo> computeLeaks() {
      List<LeakInfo> leaks = new ArrayList<>();
      for (LeakInfo leakInfo : runningThreads.values()) {
         if (leakInfo.shouldReport() && leakInfo.thread.isAlive() && !ignore(leakInfo.thread)) {
            leaks.add(leakInfo);
         }
      }
      return leaks;
   }

   private static boolean ignore(Thread thread) {
      // System threads (running before the first test) have no potential owners
      String threadName = thread.getName();
      if (IGNORED_THREADS_REGEX.matcher(threadName).matches())
         return true;

      if (thread.getName().startsWith("Thread-")) {
         // Special check for ByteMan, because nobody calls TransformListener.terminate()
         if (thread.getClass().getName().equals("org.jboss.byteman.agent.TransformListener"))
            return true;

         // Special check for Arquillian, because it uses an unnamed thread to read from the container console
         StackTraceElement[] s = thread.getStackTrace();
         for (StackTraceElement ste : s) {
            if (ste.getClassName().equals(ARQUILLIAN_CONSOLE_CONSUMER)) {
               return true;
            }
         }
   }
         return false;
   }

   private static String prettyPrintStacktrace(StackTraceElement[] stackTraceElements) {
      StringBuilder sb = new StringBuilder();
      for (StackTraceElement ste : stackTraceElements) {
         sb.append("\tat ").append(ste).append('\n');
      }
      return sb.toString();
   }

   private static Set<Thread> getThreadsSnapshot() {
      ThreadGroup group = Thread.currentThread().getThreadGroup();
      while (group.getParent() != null) {
         group = group.getParent();
      }

      int capacity = group.activeCount() * 2;
      while (true) {
         Thread[] threadsArray = new Thread[capacity];
         int count = group.enumerate(threadsArray, true);
         if (count < capacity)
            return Arrays.stream(threadsArray, 0, count).collect(Collectors.toSet());

         capacity = count * 2;
      }
   }

   /**
    * Ignore threads matching a predicate.
    */
   public static void ignoreThreadsMatching(Predicate<Thread> filter) {
      // Update the thread ownership information
      Set<Thread> currentThreads = getThreadsSnapshot();
      for (Thread thread : currentThreads) {
         if (filter.test(thread)) {
            ignoreThread(thread);
         }
      }
   }

   /**
    * Ignore a running thread.
    */
   public static void ignoreThread(Thread thread) {
      LeakInfo leakInfo = runningThreads.computeIfAbsent(thread, k -> new LeakInfo(thread, IGNORED));
   }

   /**
    * Ignore threads containing a regex.
    */
   public static void ignoreThreadsContaining(String threadNameRegex) {
      Pattern pattern = Pattern.compile(".*" + threadNameRegex + ".*");
      ignoreThreadsMatching(thread -> pattern.matcher(thread.getName()).matches());
   }

   private static class LeakInfo {
      final Thread thread;
      final LeakException stacktrace;
      boolean reported;

      LeakInfo(Thread thread, LeakException stacktrace) {
         this.thread = thread;
         this.stacktrace = stacktrace;
      }

      void markReported() {
         reported = true;
      }

      boolean shouldReport() {
         return stacktrace != IGNORED && !reported;
      }

      @Override
      public String toString() {
         String owners;
         if (stacktrace == IGNORED) {
            owners = "ignored";
         } else {
            owners = "created by " + stacktrace.getMessage();
         }
         return "{" + thread.getName() + ": " + owners + "}";
      }
   }

   private static class LeakException extends Exception {
      private static final long serialVersionUID = 2192447894828825555L;

      LeakException(String testName) {
         super(testName, null, false, false);
      }

      LeakException(String message, LeakException parent) {
         super(message + " << " + parent.getMessage(), parent, false, true);
      }
   }
}
