package org.infinispan.test.fwk;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of resources created by tests and cleans them up at the end of the test.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class TestResourceTracker {
   private static final Log log = LogFactory.getLog(TestResourceTracker.class);
   private static final EquivalentConcurrentHashMapV8<String, TestResources> testResources = new EquivalentConcurrentHashMapV8<>(AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
   private static final ThreadLocal<String> threadTestName = new ThreadLocal<>();

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

   protected static void cleanUpResources(String testName) {
      TestResources resources = testResources.remove(testName);
      if (resources != null) {
         for (Cleaner<?> cleaner : resources.getCleaners()) {
            try {
               cleaner.close();
            } catch (Throwable t) {
               log.fatalf(t, "Error cleaning resource %s for test %s", cleaner.ref, testName);
               throw new IllegalStateException("Error cleaning resource " + cleaner.ref + " for test " + testName);
            }
         }
      }
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
         String pattern = "TestNGInvoker-";
         if (threadName.startsWith(pattern)) {
            // This is a timeout test, so force the user to call our marking method
            throw new IllegalStateException("Test name is not set! Please call TestResourceTracker.testStarted(this) in your test method!");
         } else if (Thread.currentThread().getName().equals("main")) {
            // Test is being run from an IDE
            testName = "main";
         } else {
            log.warnf("Test name not set in unknown thread %s. Consider using TestResourceTracker.backgroundTestStarted(this) in your test method.", Thread.currentThread().getName());
            testName = "unknown";
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
      if (!testName.equals(getCurrentTestName())) {
         cleanUpResources(getCurrentTestName());
         throw new IllegalArgumentException("Current thread name was not set correctly: " + getCurrentTestName() +
               ", should have been " + testName);
      }
      setThreadTestName(null);
   }

   /**
    * Should be called by the user on any "background" test thread that creates resources, e.g. at the beginning of a
    * test with a {@code @Test(timeout=n)} annotation.
    */
   public static void backgroundTestStarted(Object testInstance) {
      setThreadTestName(testInstance.getClass().getName());
      Thread.currentThread().setName(getNextTestThreadName());
   }

   public static void setThreadTestName(String testName) {
      threadTestName.set(testName);
   }

   public static String getNextNodeName() {
      String testName = getCurrentTestName();
      TestResources resources = getTestResources(testName);
      String simpleName = resources.getSimpleName();
      int nextNodeIndex = resources.addNode();
      return simpleName + "-" + "Node" + getNameForIndex(nextNodeIndex);
   }

   public static String getNextTestThreadName() {
      String testName = getCurrentTestName();
      TestResources resources = getTestResources(testName);
      String simpleName = resources.getSimpleName();
      int nextThreadIndex = resources.addThread();
      return "testng-" + simpleName + (nextThreadIndex != 0 ? "-" + nextThreadIndex : "");
   }

   public static String getNameForIndex(int i) {
      final int k = 'Z' - 'A' + 1;
      String c = String.valueOf((char) ('A' + i % k));
      int q = i / k;
      return q == 0 ? c : getNameForIndex(q - 1) + c;
   }

   private static TestResources getTestResources(final String testName) {
      return testResources.computeIfAbsent(testName, new EquivalentConcurrentHashMapV8.Fun<String, TestResources>() {
         @Override
         public TestResources apply(String key) {
            return new TestResources(getSimpleName(testName));
         }
      });
   }

   private static String getSimpleName(String fullTestName) {
      return fullTestName.substring(fullTestName.lastIndexOf(".") + 1);
   }

   private static class TestResources {
      private final String simpleName;
      private final AtomicInteger nodeCount = new AtomicInteger(0);
      private final AtomicInteger threadCount = new AtomicInteger(0);
      private final List<Cleaner<?>> resourceCleaners = Collections.synchronizedList(new ArrayList<Cleaner<?>>());

      private TestResources(String simpleName) {
         this.simpleName = simpleName;
      }

      public String getSimpleName() {
         return simpleName;
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

   public static abstract class Cleaner<T> {
      protected final T ref;

      protected Cleaner(T ref) {
         this.ref = ref;
      }

      public abstract void close();
   }

   public static class CacheManagerCleaner extends Cleaner<EmbeddedCacheManager> {

      protected CacheManagerCleaner(EmbeddedCacheManager ref) {
         super(ref);
      }

      @Override
      public void close() {
         PrivilegedAction<Object> action = new PrivilegedAction<Object>() {
            @Override
            public Object run() {
               if (!ref.getStatus().isTerminated()) {
                  log.debugf("Stopping cache manager %s", ref);
                  ref.stop();
               }
               return null;
            }
         };
         if (System.getSecurityManager() != null) {
            AccessController.doPrivileged(action);
         } else {
            Security.doPrivileged(action);
         }
      }
   }
}
