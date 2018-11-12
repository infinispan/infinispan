package org.infinispan.executors;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;

/**
 * A delegating scheduled executor that lazily constructs and initializes the underlying scheduled executor, since
 * unused JDK executors are expensive.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class LazyInitializingScheduledExecutorService extends ManageableExecutorService<ScheduledExecutorService> implements ScheduledExecutorService {
   private final ThreadPoolExecutorFactory<ScheduledExecutorService> executorFactory;
   private final ThreadFactory threadFactory;

   public LazyInitializingScheduledExecutorService(
         ThreadPoolExecutorFactory<ScheduledExecutorService> executorFactory, ThreadFactory threadFactory) {
      this.executorFactory = executorFactory;
      this.threadFactory = threadFactory;
   }

   private void initIfNeeded() {
      if (executor == null) {
         synchronized (this) {
            if (executor == null) {
               executor = executorFactory.createExecutor(threadFactory);
            }
         }
      }
   }

   @Override
   public void shutdown() {
      if (executor != null) executor.shutdown();
   }

   @Override
   public List<Runnable> shutdownNow() {
      if (executor == null)
         return Collections.emptyList();
      else
         return executor.shutdownNow();
   }

   @Override
   public boolean isShutdown() {
      return executor == null || executor.isShutdown();
   }

   @Override
   public boolean isTerminated() {
      return executor == null || executor.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      if (executor == null)
         return true;
      else
         return executor.awaitTermination(timeout, unit);

   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      initIfNeeded();
      return executor.submit(task);
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      initIfNeeded();
      return executor.submit(task, result);
   }

   @Override
   public Future<?> submit(Runnable task) {
      initIfNeeded();
      return executor.submit(task);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      initIfNeeded();
      return executor.invokeAll(tasks);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      initIfNeeded();
      return executor.invokeAll(tasks, timeout, unit);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      initIfNeeded();
      return executor.invokeAny(tasks);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      initIfNeeded();
      return executor.invokeAny(tasks, timeout, unit);
   }

   @Override
   public void execute(Runnable command) {
      initIfNeeded();
      executor.execute(command);
   }

   @Override
   public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      initIfNeeded();
      return executor.schedule(command, delay, unit);
   }

   @Override
   public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      initIfNeeded();
      return executor.schedule(callable, delay, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      initIfNeeded();
      return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      initIfNeeded();
      return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
   }
}
