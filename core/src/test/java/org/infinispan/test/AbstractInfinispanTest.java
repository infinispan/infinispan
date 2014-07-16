package org.infinispan.test;

import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;

import javax.transaction.TransactionManager;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertTrue;

/**
 * AbstractInfinispanTest is a superclass of all Infinispan tests.
 *
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class AbstractInfinispanTest {

   protected final Log log = LogFactory.getLog(getClass());

   private final TrackingThreadFactory defaultThreadFactory = new TrackingThreadFactory(
         getTestThreadFactory("ForkThread-"));
   private final ExecutorService defaultExecutorService = Executors.newCachedThreadPool(defaultThreadFactory);
   public static final TimeService TIME_SERVICE = new DefaultTimeService();

   @AfterTest(alwaysRun = true)
   protected void killSpawnedThreads() {
      List<Runnable> runnables = defaultExecutorService.shutdownNow();
      log.errorf("There were runnables %s left uncompleted in test %s", runnables, getClass().getSimpleName());

      Set<Thread> threads = defaultThreadFactory.getCreatedThreads();
      for (Thread t : threads) {
         if (t.isAlive()) {
            log.warnf("There was a thread % still alive after test completion - interrupted it", t);
            t.interrupt();
         }
      }
   }

   protected void eventually(Condition ec, long timeout) {
      eventually(ec, timeout, 10);
   }

   protected void eventually(Condition ec, long timeout, int loops) {
      if (loops <= 0) {
         throw new IllegalArgumentException("Number of loops must be positive");
      }
      long sleepDuration = timeout / loops;
      if (sleepDuration == 0) {
         sleepDuration = 1;
      }
      try {
         for (int i = 0; i < loops; i++) {

            if (ec.isSatisfied()) return;
            Thread.sleep(sleepDuration);
         }
         assertTrue(ec.isSatisfied());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   /**
    * This method will actually spawn a fresh thread and will not use underlying pool.  The
    * {@link org.infinispan.test.AbstractInfinispanTest#fork(Runnable, boolean)} should be preferred
    * unless you require explicit access to the thread.
    * @param r The runnable to run
    * @param waitForCompletion Whether or not we should wait for the thread to complete before returning
    * @return The created thread
    */
   protected Thread inNewThread(Runnable r, boolean waitForCompletion) {
      final Thread t = defaultThreadFactory.newThread(new RunnableWrapper(r));
      log.tracef("About to start thread '%s' as child of thread '%s'", t.getName(), Thread.currentThread().getName());
      t.start();
      if (waitForCompletion) {
         try {
            t.join(TimeUnit.SECONDS.toMillis(10));
         } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected!", e);
         }
      }
      return t;
   }

   protected Future<?> fork(Runnable r, boolean waitForCompletion) {
      Future<?> future = defaultExecutorService.submit(new RunnableWrapper(r));
      if (waitForCompletion) {
         try {
            future.get(10, TimeUnit.SECONDS);
         } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Unexpected!", e);
         }
      }
      return future;
   }

   protected <T> Future<T> fork(Runnable r, T result) {
      return defaultExecutorService.submit(new RunnableWrapper(r), result);
   }

   protected <T> Future<T> fork(Callable<T> c) {
      return defaultExecutorService.submit(new LoggingCallable<>(c));
   }

   /**
    * Returns an executor service that can  be used by tests which has it's lifecycle handled
    * by the test container itself.
    * @param <V> The desired type provided by the user of the CompletionService
    * @return The completion service that should be used by tests
    */
   protected <V> CompletionService<V> completionService() {
      return new ExecutorCompletionService<>(defaultExecutorService);
   }

   /**
    * This should normally not be used, use the {@link AbstractInfinispanTest#completionService()}
    * method when an executor is required.  Although if you want a limited set of threads this could
    * still be useful, however the lifecycle of such threads should be guaranteed to stop by properly
    * interrupting any threads that may remain in a finally block.
    * @param prefix The prefix starting for the thread factory
    * @return A thread factory that will use the same naming schema as the other methods
    */
   protected ThreadFactory getTestThreadFactory(final String prefix) {
      final String className = getClass().getSimpleName();

      return new ThreadFactory() {
         private final AtomicInteger counter = new AtomicInteger(0);

         @Override
         public Thread newThread(Runnable r) {
            String threadName = prefix + "-" + counter.incrementAndGet() + "," + className;
            return new Thread(r, threadName);
         }
      };
   }

   /**
    * This will run the various callables at approximately the same time by ensuring they all start before
    * allowing the callables to proceed.  The callables may not be running yet at the offset of the return of
    * the completion service.
    *
    * This method doesn't allow distinction between which Future applies to which Callable.  If that is required
    * it is suggested to use {@link AbstractInfinispanTest#completionService()} and retrieve the future
    * directly when submitting the Callable while also wrapping your callables in something like
    * {@link org.infinispan.test.AbstractInfinispanTest.ConcurrentCallable} to ensure your callables will be invoked
    * at approximately the same time.
    * @param task1 First task to add - required to ensure at least 2 callables are provided
    * @param task2 Second task to add - required to ensure at least 2 callables are provided
    * @param tasks The callables to run
    * @param <V> The type of callables
    * @return The completion service that can be used to query when a callable completes
    */
   protected <V> CompletionService<V> runConcurrentlyWithCompletionService(Callable<? extends V> task1,
                                                                           Callable<? extends V> task2,
                                                                           Callable<? extends V>... tasks) {
      if (task1 == null) {
         throw new NullPointerException("Task1 was not provided!");
      }
      if (task2 == null) {
         throw new NullPointerException("Task2 was not provided!");
      }
      CompletionService<V> completionService = completionService();
      CyclicBarrier barrier = new CyclicBarrier(tasks.length);
      completionService.submit(new ConcurrentCallable<>(new LoggingCallable<>(task1), barrier));
      completionService.submit(new ConcurrentCallable<>(new LoggingCallable<>(task2), barrier));
      for (int i = 0; i < tasks.length; i++) {
         completionService.submit(new ConcurrentCallable<>(new LoggingCallable<>(tasks[i]), barrier));
      }

      return completionService;
   }

   /**
    * This will run the various callables at approximately the same time by ensuring they all start before
    * allowing the callables to proceed.  All callables will be ensure to completion else a TimeoutException will be thrown
    * @param task1 First task to add - required to ensure at least 2 callables are provided
    * @param task2 Second task to add - required to ensure at least 2 callables are provided
    * @param tasks The callable to run
    * @param <V> The type of callabels
    * @throws InterruptedException Thrown if this thread is interrupted
    * @throws ExecutionException Thrown if one of the callables throws any kind of Throwable.  The
    *         thrown Throwable will be wrapped by this exception
    * @throws TimeoutException If one of the callables doesn't complete within 10 seconds
    */
   protected <V> void runConcurrently(Callable<? extends V> task1, Callable<? extends V> task2,
                                      Callable<? extends V>... tasks) throws InterruptedException,ExecutionException,
                                                                             TimeoutException {
      CompletionService<V> completionService = runConcurrentlyWithCompletionService(task1, task2, tasks);

      // check for any errors
      ExecutionException exception = null;
      for (int i = 0; i < tasks.length; i++) {
         try {
            Future<V> future = completionService.poll(10, TimeUnit.SECONDS);
            // No timeout since it is guaranteed to be done
            future.get();
         } catch (ExecutionException e) {
            log.debug("Exception in concurrent task", e);
            exception = e;
         }
      }

      // If there was an exception throw the last one
      if (exception != null) {
         throw exception;
      }
   }

   /**
    * This will run the callable at approximately the same time in different thrads by ensuring they all start before
    * allowing the callables to proceed.  All callables will be ensure to completion else a TimeoutException will be thrown
    *
    * The callable itself should be thread safe since it will be invoked from multiple threads.
    * @param callable The callable to run multiple times
    * @param <V> The type of callable
    * @throws InterruptedException Thrown if this thread is interrupted
    * @throws ExecutionException Thrown if one of the callables throws any kind of Throwable.  The
    *         thrown Throwable will be wrapped by this exception
    * @throws TimeoutException If one of the callables doesn't complete within 10 seconds
    */
   protected <V> void runConcurrently(Callable<V> callable, int invocationCount) throws InterruptedException,
                                                                                        ExecutionException,
                                                                                        TimeoutException {
      CompletionService<V> completionService = completionService();
      CyclicBarrier barrier = new CyclicBarrier(invocationCount);

      for (int i = 0; i < invocationCount; ++i) {
         completionService.submit(new ConcurrentCallable<>(new LoggingCallable<>(callable), barrier));
      }

      // check for any errors
      ExecutionException exception = null;
      for (int i = 0; i < invocationCount; ++i) {
         try {
            completionService.poll(10, TimeUnit.SECONDS).get();
         } catch (ExecutionException e) {
            log.debug("Exception in concurrent task", e);
            exception = e;
         }
      }

      // If there was an exception throw the last one
      if (exception != null) {
         throw exception;
      }
   }

   private final class TrackingThreadFactory implements ThreadFactory {
      private final ThreadFactory realFactory;
      private final Set<Thread> createdThreads = new ConcurrentHashSet<>();

      public TrackingThreadFactory(ThreadFactory factory) {
         this.realFactory = factory;
      }

      @Override
      public Thread newThread(Runnable r) {
         Thread thread = realFactory.newThread(r);
         createdThreads.add(thread);
         return thread;
      }

      public Set<Thread> getCreatedThreads() {
         return createdThreads;
      }
   }

   /**
    * A callable that will first await on the provided barrier before calling the provided callable.
    * This is useful to have a better attempt at multiple threads ran at the same time, but still is
    * no guarantee since this is controlled by the thread scheduler.
    * @param <V>
    */
   public final class ConcurrentCallable<V> implements Callable<V> {
      private final Callable<? extends V> callable;
      private final CyclicBarrier barrier;

      public ConcurrentCallable(Callable<? extends V> callable, CyclicBarrier barrier) {
         this.callable = callable;
         this.barrier = barrier;
      }

      @Override
      public V call() throws Exception {
         log.trace("About to wait on provided barrier");
         try {
            barrier.await(10, TimeUnit.SECONDS);
            log.trace("Completed await on provided barrier");
         } catch (InterruptedException | TimeoutException | BrokenBarrierException e) {
            log.tracef("Barrier await was broken due to exception", e);
         }
         return callable.call();
      }
   }

   public final class RunnableWrapper implements Runnable {

      final Runnable realOne;

      public RunnableWrapper(Runnable realOne) {
         this.realOne = realOne;
      }

      @Override
      public void run() {
         try {
            log.trace("Started fork runnable..");
            realOne.run();
         } catch (Throwable e) {
            log.trace("Exiting fork runnable due to exception", e);
         } finally {
            log.trace("Exiting fork runnable.");
         }
      }
   }


   protected void eventually(Condition ec) {
      eventually(ec, 10000);
   }

   protected interface Condition {
      public boolean isSatisfied() throws Exception;
   }

   private class LoggingCallable<T> implements Callable<T> {
      private final Callable<? extends T> c;

      public LoggingCallable(Callable<? extends T> c) {
         this.c = c;
      }

      @Override
      public T call() throws Exception {
         try {
            log.trace("Started fork callable..");
            return c.call();
         } catch (Exception e) {
            log.trace("Exiting fork callable due to exception", e);
            throw e;
         } finally {
            log.trace("Exiting fork callable.");
         }
      }
   }

   public void safeRollback(TransactionManager transactionManager) {
      try {
         transactionManager.rollback();
      } catch (Exception e) {
         //ignored
      }
   }
}
