package org.infinispan.commons.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
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
                      // atomic-factory Container
                      "|AtomicFactory-global" +
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
                      // RMI connectionThreadPool threads
                      "|RMI " +
                      // jboss-modules
                      "|Reference Reaper" +
                      // jboss-remoting
                      "|remoting-jmx client" +
                      ").*");
   private static final String ARQUILLIAN_CONSOLE_CONSUMER =
      "org.jboss.as.arquillian.container.managed.ManagedDeployableContainer$ConsoleConsumer";

   private static Logger log = Logger.getLogger(ThreadLeakChecker.class);
   private static volatile long lastUpdate = 0;
   private static final Set<String> runningTests = ConcurrentHashMap.newKeySet();
   private static final BlockingQueue<String> finishedTests = new LinkedBlockingDeque<>();
   private static final Map<Thread, LeakInfo> runningThreads = new ConcurrentHashMap<>();
   private static final Lock lock = new ReentrantLock();

   /**
    * A test class has started, and we should consider it as a potential owner for new threads.
    */
   public static void testStarted(String testName) {
      lock.lock();
      try {
         runningTests.add(testName);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Save the system threads in order to ignore them
    */
   public static void saveInitialThreads() {
      lock.lock();
      try {
         Set<Thread> currentThreads = getThreadsSnapshot();
         for (Thread thread : currentThreads) {
            LeakInfo leakInfo = new LeakInfo(thread, Collections.emptyList());
            leakInfo.ignore();
            runningThreads.putIfAbsent(thread, leakInfo);
         }
         lastUpdate = System.nanoTime();
      } finally {
         lock.unlock();
      }
   }

   /**
    * A test class has finished, and we should not consider it a potential owner for new threads any more.
    */
   public static void testFinished(String testName) {
      lock.lock();
      try {
         finishedTests.add(testName);

         // Only update threads once per second, unless this is the last test
         // or the test suite runs on a single thread
         boolean noRunningTest = runningTests.size() <= finishedTests.size();
         if (!noRunningTest && (System.nanoTime() - lastUpdate < TimeUnit.SECONDS.toNanos(1)))
            return;

         lastUpdate = System.nanoTime();

         // Available owners are tests that were running at any point since the last check
         List<String> availableOwners = new ArrayList<>(runningTests);
         runningTests.removeAll(finishedTests);
         finishedTests.clear();
         updateThreadOwnership(availableOwners);
      } finally {
         lock.unlock();
      }
   }

   public static void updateThreadOwnership(List<String> availableOwners) {
      // Update the thread ownership information
      Set<Thread> currentThreads = getThreadsSnapshot();
      runningThreads.keySet().retainAll(currentThreads);
      for (Thread thread : currentThreads) {
         runningThreads.putIfAbsent(thread, new LeakInfo(thread, availableOwners));
      }
   }

   /**
    * Check for leaked threads.
    *
    * <p>When running tests in parallel, this method will only perform the leak check if there are no running tests.</p>
    *
    * @param testName test that just ended, or {@code null} if running after all the tests (e.g. from {@code
    * @AfterSuite} in TestNG)
    */
   public static void checkForLeaks(String testName) {
      lock.lock();
      try {
         if (testName == null) {
            assert runningTests.isEmpty() : "Tests are still running: " + runningTests;
         } else if (runningTests.size() > 1) {
            return;
         } else if (runningTests.size() == 1) {
            assert runningTests.contains(testName) :
               "Test " + runningTests + " is running, should have been " + testName;
            testFinished(testName);
         }
         performCheck();
      } finally {
         lock.unlock();
      }
   }

   private static void performCheck() {
      updateThreadOwnership(Collections.singletonList("UNKNOWN"));
      List<LeakInfo> leaks = computeLeaks();

      if (!leaks.isEmpty()) {
         // Give the threads some more time to finish, in case the stop method didn't wait
         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
            // Ignore
            Thread.currentThread().interrupt();
         }
         // Update the thread ownership information
         updateThreadOwnership(Collections.singletonList("UNKNOWN"));
         leaks = computeLeaks();
      }

      if (!leaks.isEmpty()) {
         for (LeakInfo leakInfo : leaks) {
            log.warnf("Possible leaked thread:\n%s", prettyPringStacktrace(leakInfo.thread));
            leakInfo.reported = true;
         }
         // Strategies for debugging test suite thread leaks
         // Use -Dinfinispan.test.parallel.threads=3 (or even less) to narrow down source tests
         // Set a conditional breakpoint in Thread.start with the name of the leaked thread
         // If the thread has the pattern of a particular component, set a conditional breakpoint in that component
         throw new AssertionError("Leaked threads: \n  " +
                                  leaks.stream()
                                       .map(Object::toString)
                                       .collect(Collectors.joining(",\n  ")));
      }
   }

   private static List<LeakInfo> computeLeaks() {
      List<LeakInfo> leaks = new ArrayList<>();
      for (LeakInfo leakInfo : runningThreads.values()) {
         if (!leakInfo.reported && leakInfo.thread.isAlive() && !ignore(leakInfo)) {
            leaks.add(leakInfo);
         }
      }
      return leaks;
   }

   private static boolean ignore(LeakInfo leakInfo) {
      // System threads (running before the first test) have no potential owners
      String threadName = leakInfo.thread.getName();
      if (leakInfo.potentialOwnerTests.isEmpty())
         return true;
      if (IGNORED_THREADS_REGEX.matcher(threadName).matches())
         return true;

      if (leakInfo.thread.getName().startsWith("Thread-")) {
         // Special check for ByteMan, because nobody calls TransformListener.terminate()
         if (leakInfo.thread.getClass().getName().equals("org.jboss.byteman.agent.TransformListener"))
            return true;

         // Special check for Arquillian, because it uses an unnamed thread to read from the container console
         StackTraceElement[] s = leakInfo.thread.getStackTrace();
         for (StackTraceElement ste : s) {
            if (ste.getClassName().equals(ARQUILLIAN_CONSOLE_CONSUMER)) {
               return true;
            }
         }
   }
         return false;
   }

   public static String prettyPringStacktrace(Thread thread) {
      // "management I/O-2" #55 prio=5 os_prio=0 tid=0x00007fe6a8134000 nid=0x7f9d runnable
      // [0x00007fe64e4db000]
      //    java.lang.Thread.State:RUNNABLE
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("\"%s\" %sprio=%d tid=0x%x nid=NA %s\n", thread.getName(),
                              thread.isDaemon() ? "daemon " : "", thread.getPriority(), thread.getId(),
                              thread.getState().toString().toLowerCase()));
      sb.append("   java.lang.Thread.State: ").append(thread.getState()).append('\n');
      StackTraceElement[] s = thread.getStackTrace();
      for (StackTraceElement ste : s) {
         sb.append("\t").append(ste).append('\n');
      }
      return sb.toString();
   }

   private static <T> List<T> drain(BlockingQueue<T> blockingQueue) {
      List<T> list = new ArrayList<>();
      blockingQueue.drainTo(list);
      return list;
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

   public static void ignoreThreadsMatching(Predicate<Thread> filter) {
      // Update the thread ownership information
      Set<Thread> currentThreads = getThreadsSnapshot();
      for (Thread thread : currentThreads) {
         if (filter.test(thread)) {
            ignoreThread(thread);
         }
      }
   }

   public static void ignoreThread(Thread thread) {
      LeakInfo leakInfo = runningThreads.computeIfAbsent(thread, k -> new LeakInfo(thread, Collections.emptyList()));
      leakInfo.ignore();
   }

   public static void ignoreThreadsContaining(String threadNameSubstring) {
      ignoreThreadsMatching(thread -> thread.getName().matches(".*" + threadNameSubstring + ".*"));
   }

   private static class LeakInfo {
      final Thread thread;
      final List<String> potentialOwnerTests;
      boolean reported;

      LeakInfo(Thread thread, List<String> potentialOwnerTests) {
         this.thread = thread;
         this.potentialOwnerTests = potentialOwnerTests;
      }

      public void ignore() {
         reported = true;
      }

      public boolean shouldReport() {
         return !reported;
      }

      @Override
      public String toString() {
         return "{" + thread.getName() + ": possible sources " + potentialOwnerTests + "}";
      }
   }
}
