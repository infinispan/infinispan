package org.infinispan.commons.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.logging.Logger;

/**
 * Keeps track of resources created by tests and cleans them up at the end of the test.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class TestResourceTracker {
   private static final Logger log = Logger.getLogger(TestResourceTracker.class);
   private static final ConcurrentMap<String, TestResources> testResources = new ConcurrentHashMap<>();
   // Inheritable to allow for tests that spawn a thread to keep the test thread name of the original
   private static final ThreadLocal<String> threadTestName = new InheritableThreadLocal<>();

   public static void addResource(String testName, final Cleaner<?> cleaner) {
      TestResources resources = getTestResources(testName);
      resources.addResource(cleaner);
   }

   /**
    * Add a resource to the current thread's running test.
    */
   public static void addResource(Cleaner<?> cleaner) {
      String testName = getCurrentTestName();
      addResource(testName, cleaner);
   }

   public static void cleanUpResources(String testName) {
      TestResources resources = testResources.remove(testName);
      if (resources != null) {
         for (Cleaner<?> cleaner : resources.getCleaners()) {
            try {
               cleaner.close();
            } catch (Throwable t) {
               log.fatalf(t, "Error cleaning resource %s for test %s", cleaner.ref, testName);
               throw new IllegalStateException("Error cleaning resource " + cleaner.ref + " for test " + testName, t);
            }
         }
      }
   }

   public static String getCurrentTestShortName() {
      String currentTestName = TestResourceTracker.getCurrentTestName();
      return getTestResources(currentTestName).getShortName();
   }

   public static String getCurrentTestName() {
      String testName = threadTestName.get();
      if (testName == null) {
         // Either we're running from within the IDE or it's a
         // @Test(timeOut=nnn) test. We rely here on some specific TestNG
         // thread naming convention which can break, but TestNG offers no
         // other alternative. It does not offer any callbacks within the
         // thread that runs the test that can timeout.
         String threadName = Thread.currentThread().getName();
         if (threadName.equals("main") || threadName.equals("TestNG")) {
            // Regular test, force the user to extend AbstractInfinispanTest
            throw new IllegalStateException("Test name is not set! Please extend AbstractInfinispanTest!");
         } else if (threadName.startsWith("TestNGInvoker-")) {
            // This is a timeout test, so force the user to call our marking method
            throw new IllegalStateException("Test name is not set! Please call TestResourceTracker.testThreadStarted(this.getTestName()) in your test method!");
         } else {
            throw new IllegalStateException("Test name is not set! Please call TestResourceTracker.testThreadStarted(this.getTestName()) in thread " + threadName + " !");
         }
      }
      return testName;
   }

   /**
    * Called on the "main" test thread, before any configuration method.
    */
   public static void testStarted(String testName) {
      if (testResources.containsKey(testName)) {
         throw new IllegalStateException("Two tests with the same name running in parallel: " + testName);
      }
      setThreadTestName(testName);
      Thread.currentThread().setName(getNextTestThreadName());
   }

   /**
    * Called on the "main" test thread, after any configuration method.
    */
   public static void testFinished(String testName) {
      cleanUpResources(testName);
      if (!testName.equals(threadTestName.get())) {
         cleanUpResources(getCurrentTestName());
         throw new IllegalArgumentException("Current thread's test name was not set correctly: " + getCurrentTestName() +
               ", should have been " + testName);
      }
      setThreadTestName(null);
   }

   /**
    * Should be called by the user on any "background" test thread that creates resources, e.g. at the beginning of a
    * test with a {@code @Test(timeout=n)} annotation.
    * @param testName
    */
   public static void testThreadStarted(String testName) {
      setThreadTestName(testName);
      Thread.currentThread().setName(getNextTestThreadName());
   }

   public static void setThreadTestName(String testName) {
      threadTestName.set(testName);
   }

   public static void setThreadTestNameIfMissing(String testName) {
      if (threadTestName.get() == null) {
         threadTestName.set(testName);
      }
   }

   public static String getNextNodeName() {
      String testName = getCurrentTestName();
      TestResources resources = getTestResources(testName);
      String shortName = resources.getShortName();
      int nextNodeIndex = resources.addNode();
      return shortName + "-" + "Node" + getNameForIndex(nextNodeIndex);
   }

   public static String getNextTestThreadName() {
      String testName = getCurrentTestName();
      TestResources resources = getTestResources(testName);
      String shortName = resources.getShortName();
      int nextThreadIndex = resources.addThread();
      return "testng-" + shortName + (nextThreadIndex != 0 ? "-" + nextThreadIndex : "");
   }

   public static String getNameForIndex(int i) {
      final int k = 'Z' - 'A' + 1;
      String c = String.valueOf((char) ('A' + i % k));
      int q = i / k;
      return q == 0 ? c : getNameForIndex(q - 1) + c;
   }

   private static TestResources getTestResources(final String testName) {
      return testResources.computeIfAbsent(testName, TestResources::new);
   }

   private static class TestResources {
      private static final boolean shortenTestName = Boolean.getBoolean("infinispan.test.shortTestName");

      private final String shortName;
      private final AtomicInteger nodeCount = new AtomicInteger(0);
      private final AtomicInteger threadCount = new AtomicInteger(0);
      private final List<Cleaner<?>> resourceCleaners = Collections.synchronizedList(new ArrayList<Cleaner<?>>());

      private TestResources(String testName) {
         if (shortenTestName) {
            this.shortName = "Test";
         } else {
            int simpleNameStart = testName.lastIndexOf(".");
            int parametersStart = testName.indexOf('[');
            this.shortName = testName.substring(simpleNameStart + 1,
                                                parametersStart > 0 ? parametersStart : testName.length());
         }
      }

      public String getShortName() {
         return shortName;
      }

      public int addNode() {
         return nodeCount.getAndIncrement();
      }

      public int addThread() {
         return threadCount.getAndIncrement();
      }

      public void addResource(Cleaner<?> cleaner) {
         resourceCleaners.add(cleaner);
      }

      public List<Cleaner<?>> getCleaners() {
         return resourceCleaners;
      }
   }

   public abstract static class Cleaner<T> {
      protected final T ref;

      protected Cleaner(T ref) {
         this.ref = ref;
      }

      public abstract void close();
   }

}
