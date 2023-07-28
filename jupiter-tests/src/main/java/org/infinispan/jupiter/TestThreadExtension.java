package org.infinispan.jupiter;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestThreadExtension implements AfterAllCallback, AfterEachCallback, BeforeAllCallback, BeforeEachCallback {

   static final Log log = LogFactory.getLog(TestThreadExtension.class);

   private final String testName;


   private final ThreadFactory defaultThreadFactory = getTestThreadFactory("ForkThread");
   private final ThreadPoolExecutor testExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
         60L, TimeUnit.SECONDS,
         new SynchronousQueue<>(),
         defaultThreadFactory);

   public TestThreadExtension(Class<?> test) {
      this.testName = test.getName();
   }

   @Override
   public void beforeAll(ExtensionContext context) {
      System.out.println("beforeAll");
      TestResourceTracker.testStarted(testName);
   }

   @Override
   public void afterAll(ExtensionContext context) {
      System.out.println("afterAll");
      killSpawnedThreads();
      TestResourceTracker.testFinished(testName);
   }

   @Override
   public void beforeEach(ExtensionContext context) throws Exception {
      System.out.println("beforeEach");
   }

   @Override
   public void afterEach(ExtensionContext context) throws Exception {
      System.out.println("afterEach");
      checkThreads();
   }

   public void cleanUpResources() {
      TestResourceTracker.cleanUpResources(testName);
   }

   public <T> Future<T> submit(Callable<T> task) {
      return testExecutor.submit(task);
   }

   /**
    * This should normally not be used, use the {@code fork(Runnable|Callable|ExceptionRunnable)}
    * method when an executor is required.
    *
    * Although if you want a limited set of threads this could still be useful for something like
    * {@link java.util.concurrent.Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)} or
    * {@link java.util.concurrent.Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}
    *
    * @param prefix The prefix starting for the thread factory
    * @return A thread factory that will use the same naming schema as the other methods
    */
   public ThreadFactory getTestThreadFactory(final String prefix) {
      final String className = getClass().getSimpleName();

      return new ThreadFactory() {
         private final AtomicInteger counter = new AtomicInteger(0);

         @Override
         public Thread newThread(Runnable r) {
            String threadName = prefix + "-" + counter.incrementAndGet() + "," + className;
            Thread thread = new Thread(r, threadName);
            TestResourceTracker.addResource(getClass().getName(), new ThreadCleaner(thread));
            return thread;
         }
      };
   }

   private void checkThreads() {
      int activeTasks = testExecutor.getActiveCount();
      if (activeTasks != 0) {
         log.errorf("There were %d active tasks found in the test executor service for class %s", activeTasks,
               getClass().getSimpleName());
      }
   }
   private void killSpawnedThreads() {
      List<Runnable> runnables = testExecutor.shutdownNow();
      if (!runnables.isEmpty()) {
         log.errorf("There were runnables %s left uncompleted in test %s", runnables, getClass().getSimpleName());
      }
   }

   private static class ThreadCleaner extends TestResourceTracker.Cleaner<Thread> {
      public ThreadCleaner(Thread thread) {
         super(thread);
      }

      @Override
      public void close() {
         if (ref.isAlive() && !ref.isInterrupted()) {
            log.warnf("There was a thread %s still alive after test completion - interrupted it",
                  ref);
            ref.interrupt();
         }
      }
   }
}
