package org.infinispan.test.executors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.executors.WrappedExecutorService;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.jboss.logging.NDC;

/**
 * Wraps an ExecutorService to preserve Log4j2 ThreadContext (MDC) and JBoss Logging NDC
 * when tasks are submitted. This ensures that thread names and cache context are properly
 * logged even when code runs on executor thread pools.
 * <p>
 * This is test-only infrastructure to improve log debugging. It's automatically loaded
 * via reflection when available on the test classpath, requiring no configuration.
 *
 * @author William Burns
 * @since 16.2
 */
public final class ThreadContextExecutorService implements ScheduledExecutorService, BlockingTaskAwareExecutorService, WrappedExecutorService {
   private final ExecutorService delegate;
   private final ScheduledExecutorService scheduledDelegate;
   private final BlockingTaskAwareExecutorService blockingDelegate;

   private ThreadContextExecutorService(ExecutorService delegate) {
      this.delegate = delegate;
      this.scheduledDelegate = delegate instanceof ScheduledExecutorService ? (ScheduledExecutorService) delegate : null;
      this.blockingDelegate = delegate instanceof BlockingTaskAwareExecutorService ? (BlockingTaskAwareExecutorService) delegate : null;
   }

   /**
    * Wraps an executor service to preserve ThreadContext.
    */
   @SuppressWarnings("unchecked")
   public static <T extends ExecutorService> T wrap(T executor) {
      if (executor == null || executor instanceof ThreadContextExecutorService) {
         return executor;
      }
      return (T) new ThreadContextExecutorService(executor);
   }

   @Override
   public ExecutorService unwrap() {
      return delegate;
   }

   @Override
   public void execute(Runnable command) {
      delegate.execute(ThreadContextCallable.wrap(command));
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      return delegate.submit(ThreadContextCallable.wrap(task));
   }

   @Override
   public Future<?> submit(Runnable task) {
      return delegate.submit(ThreadContextCallable.wrap(task));
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      return delegate.submit(ThreadContextCallable.wrap(task), result);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return delegate.invokeAll(wrapCollection(tasks));
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
         throws InterruptedException {
      return delegate.invokeAll(wrapCollection(tasks), timeout, unit);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
         throws InterruptedException, ExecutionException {
      return delegate.invokeAny(wrapCollection(tasks));
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
         throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.invokeAny(wrapCollection(tasks), timeout, unit);
   }

   @Override
   public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      if (scheduledDelegate == null) {
         throw new UnsupportedOperationException("Not a ScheduledExecutorService");
      }
      return scheduledDelegate.schedule(ThreadContextCallable.wrap(command), delay, unit);
   }

   @Override
   public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      if (scheduledDelegate == null) {
         throw new UnsupportedOperationException("Not a ScheduledExecutorService");
      }
      return scheduledDelegate.schedule(ThreadContextCallable.wrap(callable), delay, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      if (scheduledDelegate == null) {
         throw new UnsupportedOperationException("Not a ScheduledExecutorService");
      }
      return scheduledDelegate.scheduleAtFixedRate(ThreadContextCallable.wrap(command), initialDelay, period, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      if (scheduledDelegate == null) {
         throw new UnsupportedOperationException("Not a ScheduledExecutorService");
      }
      return scheduledDelegate.scheduleWithFixedDelay(ThreadContextCallable.wrap(command), initialDelay, delay, unit);
   }

   @Override
   public void shutdown() {
      delegate.shutdown();
   }

   @Override
   public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
   }

   @Override
   public boolean isShutdown() {
      return delegate.isShutdown();
   }

   @Override
   public boolean isTerminated() {
      return delegate.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
   }

   @Override
   public void execute(BlockingRunnable runnable) {
      if (blockingDelegate == null) {
         throw new UnsupportedOperationException("Not a BlockingTaskAwareExecutorService");
      }
      blockingDelegate.execute(wrapBlockingRunnable(runnable));
   }

   @Override
   public void checkForReadyTasks() {
      if (blockingDelegate == null) {
         throw new UnsupportedOperationException("Not a BlockingTaskAwareExecutorService");
      }
      blockingDelegate.checkForReadyTasks();
   }

   private static BlockingRunnable wrapBlockingRunnable(BlockingRunnable runnable) {
      if (runnable == null) {
         return null;
      }
      return new ThreadContextBlockingRunnable(runnable);
   }

   private static <T> Collection<Callable<T>> wrapCollection(Collection<? extends Callable<T>> tasks) {
      List<Callable<T>> wrapped = new ArrayList<>(tasks.size());
      for (Callable<T> task : tasks) {
         wrapped.add(ThreadContextCallable.wrap(task));
      }
      return wrapped;
   }

   private static final class ThreadContextBlockingRunnable implements BlockingRunnable {
      private final BlockingRunnable delegate;
      private final java.util.Map<String, String> context;
      private final String ndcStack;

      ThreadContextBlockingRunnable(BlockingRunnable delegate) {
         this.delegate = delegate;
         this.context = org.apache.logging.log4j.ThreadContext.getImmutableContext();
         this.ndcStack = NDC.get();
      }

      @Override
      public boolean isReady() {
         return delegate.isReady();
      }

      @Override
      public void run() {
         java.util.Map<String, String> previousContext = org.apache.logging.log4j.ThreadContext.getImmutableContext();
         String previousNdc = NDC.get();

         org.apache.logging.log4j.ThreadContext.clearAll();
         NDC.clear();

         if (!context.isEmpty()) {
            org.apache.logging.log4j.ThreadContext.putAll(context);
         }
         if (ndcStack != null) {
            NDC.push(ndcStack);
         }

         try {
            delegate.run();
         } finally {
            org.apache.logging.log4j.ThreadContext.clearAll();
            NDC.clear();

            if (!previousContext.isEmpty()) {
               org.apache.logging.log4j.ThreadContext.putAll(previousContext);
            }
            if (previousNdc != null) {
               NDC.push(previousNdc);
            }
         }
      }
   }
}
