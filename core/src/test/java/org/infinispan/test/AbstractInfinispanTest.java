package org.infinispan.test;

import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;

import javax.transaction.TransactionManager;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertTrue;


/**
 * AbstractInfinispanTest is a superclass of all Infinispan tests.
 *
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class AbstractInfinispanTest {

   protected final Log log = LogFactory.getLog(getClass());

   private final Set<TrackingThreadFactory> requestedThreadFactories = new ConcurrentHashSet<>();

   private final TrackingThreadFactory defaultThreadFactory = (TrackingThreadFactory)getTestThreadFactory("ForkThread");
   private final ThreadPoolExecutor defaultExecutorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                                                                 60L, TimeUnit.SECONDS,
                                                                                 new SynchronousQueue<Runnable>(),
                                                                                 defaultThreadFactory);

   public static final TimeService TIME_SERVICE = new DefaultTimeService();

   @AfterTest(alwaysRun = true)
   protected void killSpawnedThreads() {
      List<Runnable> runnables = defaultExecutorService.shutdownNow();
      if (!runnables.isEmpty()) {
         log.errorf("There were runnables %s left uncompleted in test %s", runnables, getClass().getSimpleName());
      }

      for (TrackingThreadFactory factory : requestedThreadFactories) {
         checkFactoryForLeaks(factory);
      }
   }

   @AfterMethod
   protected void checkThreads() {
      int activeTasks = defaultExecutorService.getActiveCount();
      if (activeTasks != 0) {
         log.errorf("There were %i active tasks found in the test executor service for class %s", activeTasks,
                    getClass().getSimpleName());
      }
   }

   private void checkFactoryForLeaks(TrackingThreadFactory factory) {
      Set<Thread> threads = factory.getCreatedThreads();
      for (Thread t : threads) {
         if (t.isAlive() && !t.isInterrupted()) {
            log.warnf("There was a thread % still alive after test completion - interrupted it", t);
            t.interrupt();
         }
      }
   }

   protected void eventually(Condition ec, long timeoutMillis) {
      eventually(ec, timeoutMillis, TimeUnit.MILLISECONDS);
   }

   /**
    * @deprecated Use {@link #eventually(Condition, long, long, TimeUnit)} instead.
    */
   @Deprecated
   protected void eventually(Condition ec, long timeoutMillis, int loops) {
      eventually(null, ec, timeoutMillis, loops);
   }

   /**
    * @deprecated Use {@link #eventually(String, Condition, long, long, TimeUnit)} instead.
    */
   @Deprecated
   protected void eventually(String message, Condition ec, long timeoutMillis, int loops) {
      if (loops <= 0) {
         throw new IllegalArgumentException("Number of loops must be positive");
      }
      long sleepDuration = timeoutMillis / loops + 1;
      eventually(message, ec, timeoutMillis, sleepDuration, TimeUnit.MILLISECONDS);
   }

   protected void eventually(Condition ec, long timeout, TimeUnit unit) {
      eventually(null, ec, timeout, 500, unit);
   }

   protected void eventually(Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(null, ec, timeout, pollInterval, unit);
   }

   protected void eventually(String message, Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      if (pollInterval <= 0) {
         throw new IllegalArgumentException("Check interval must be positive");
      }
      try {
         long expectedEndTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
         long sleepMillis = TimeUnit.MILLISECONDS.convert(pollInterval, unit);
         while (expectedEndTime - System.nanoTime() > 0) {
            if (ec.isSatisfied()) return;
            Thread.sleep(sleepMillis);
         }
         assertTrue(message, ec.isSatisfied());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   /**
    * This method will actually spawn a fresh thread and will not use underlying pool.  The
    * {@link org.infinispan.test.AbstractInfinispanTest#fork(Runnable)} should be preferred
    * unless you require explicit access to the thread.
    * @param r The runnable to run
    * @return The created thread
    */
   protected Thread inNewThread(Runnable r) {
      final Thread t = defaultThreadFactory.newThread(new RunnableWrapper(r));
      log.tracef("About to start thread '%s' as child of thread '%s'", t.getName(), Thread.currentThread().getName());
      t.start();
      return t;
   }

   protected Future<?> fork(Runnable r) {
      return defaultExecutorService.submit(new RunnableWrapper(r));
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
    * still be useful for something like {@link java.util.concurrent.Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)} or
    * {@link java.util.concurrent.Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}
    * @param prefix The prefix starting for the thread factory
    * @return A thread factory that will use the same naming schema as the other methods
    */
   protected ThreadFactory getTestThreadFactory(final String prefix) {
      TrackingThreadFactory ttf = new TrackingThreadFactory(getRealThreadFactory(prefix));
      requestedThreadFactories.add(ttf);
      return ttf;
   }

   private ThreadFactory getRealThreadFactory(final String prefix) {
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
      Callable[] callables = new Callable[tasks.length + 2];
      int i = 0;
      callables[i++] = task1;
      callables[i++] = task2;
      for (Callable<? extends V> callable : tasks) {
         callables[i++] = callable;
      }
      return runConcurrentlyWithCompletionService(callables);
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
    * @param tasks The callables to run.  Note the type isn't provided since it is difficult
    * @param <V> The type of callables
    * @return The completion service that can be used to query when a callable completes
    */
   protected <V> CompletionService<V> runConcurrentlyWithCompletionService(Callable[] tasks) {
      if (tasks.length == 0) {
         throw new IllegalArgumentException("Provided tasks array was empty");
      }
      CompletionService<V> completionService = completionService();
      CyclicBarrier barrier = new CyclicBarrier(tasks.length);
      for (Callable task : tasks) {
         completionService.submit(new ConcurrentCallable<>(new LoggingCallable<V>(task), barrier));
      }

      return completionService;
   }

   /**
    * This will run the various callables at approximately the same time by ensuring they all start before
    * allowing the callables to proceed.  All callables will be ensure to completion else a TimeoutException will be thrown
    * @param task1 First task to add - required to ensure at least 2 callables are provided
    * @param task2 Second task to add - required to ensure at least 2 callables are provided
    * @param tasks The callables to run
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

      waitForCompletionServiceTasks(completionService, tasks.length + 2);
   }

   /**
    * This will run the various callables at approximately the same time by ensuring they all start before
    * allowing the callables to proceed.  All callables will be ensure to completion else a TimeoutException will be thrown
    * @param tasks The callables to run
    * @param <V> The type of callables
    * @throws InterruptedException Thrown if this thread is interrupted
    * @throws ExecutionException Thrown if one of the callables throws any kind of Throwable.  The
    *         thrown Throwable will be wrapped by this exception
    * @throws TimeoutException If one of the callables doesn't complete within 10 seconds
    */
   protected <V> void runConcurrently(Callable<? extends V>[] tasks) throws InterruptedException,ExecutionException,
                                                                             TimeoutException {
      CompletionService<V> completionService = runConcurrentlyWithCompletionService(tasks);

      waitForCompletionServiceTasks(completionService, tasks.length);
   }

   /**
    * Waits for the desired numberOfTasks in the completion service to complete one at a time polling for each up to
    * 10 seconds.  If one of the tasks produces an exception, the first one is rethrown wrapped in a ExecutionException
    * after all tasks have completed.
    * @param completionService The completion service that had tasks submitted to that total numberOfTasks
    * @param numberOfTasks How many tasks have been sent to the service
    * @throws InterruptedException If this thread is interruped while waiting for the tasks to complete
    * @throws ExecutionException Thrown if one of the tasks produces an exception.  The first encounted exception will
    *         be thrown
    * @throws TimeoutException If a task is not found to complete within 10 seconds of the previously completed or the
    *         first task.
    */
   protected void waitForCompletionServiceTasks(CompletionService<?> completionService, int numberOfTasks)
         throws InterruptedException,ExecutionException, TimeoutException {
      if (completionService == null) {
         throw new NullPointerException("Provided completionService cannot be null");
      }
      if (numberOfTasks <= 0) {
         throw new IllegalArgumentException("Provided numberOfTasks cannot be less than or equal to 0");
      }
      // check for any errors
      ExecutionException exception = null;
      for (int i = 0; i < numberOfTasks; i++) {
         try {
            Future<?> future = completionService.poll(10, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException("Concurrent task didn't complete within 10 seconds!");
            }
            // No timeout since it is guaranteed to be done
            future.get();
         } catch (ExecutionException e) {
            log.debug("Exception in concurrent task", e);
            if (exception == null) {
               exception = e;
            }
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
      if (callable == null) {
         throw new NullPointerException("Provided callable cannot be null");
      }
      if (invocationCount <= 0) {
         throw new IllegalArgumentException("Provided invocationCount cannot be less than or equal to 0");
      }
      CompletionService<V> completionService = completionService();
      CyclicBarrier barrier = new CyclicBarrier(invocationCount);

      for (int i = 0; i < invocationCount; ++i) {
         completionService.submit(new ConcurrentCallable<>(new LoggingCallable<>(callable), barrier));
      }

      waitForCompletionServiceTasks(completionService, invocationCount);
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
            log.debug("Exiting fork runnable.");
         } catch (Throwable e) {
            log.debug("Exiting fork runnable due to exception", e);
         }
      }
   }


   protected void eventually(Condition ec) {
      eventually(ec, 10000);
   }

   protected void eventually(String message, Condition ec) {
      eventually(message, ec, 10000, 500, TimeUnit.MILLISECONDS);
   }

   protected interface Condition {
      boolean isSatisfied() throws Exception;
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
            T result = c.call();
            log.debug("Exiting fork callable.");
            return result;
         } catch (Exception e) {
            log.debug("Exiting fork callable due to exception", e);
            throw e;
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
