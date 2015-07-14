package org.infinispan.executors;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A delegating executor that lazily constructs and initializes the underlying executor.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public final class LazyInitializingBlockingTaskAwareExecutorService implements BlockingTaskAwareExecutorService {

   private final ThreadPoolExecutorFactory<ExecutorService> executorFactory;
   private final ThreadFactory threadFactory;
   private final TimeService timeService;
   private final String controllerThreadName;
   private volatile BlockingTaskAwareExecutorService delegate;

   public LazyInitializingBlockingTaskAwareExecutorService(ThreadPoolExecutorFactory<ExecutorService> executorFactory,
                                                           ThreadFactory threadFactory,
                                                           TimeService timeService, String controllerThreadName) {
      this.executorFactory = executorFactory;
      this.threadFactory = threadFactory;
      this.timeService = timeService;
      this.controllerThreadName = controllerThreadName;
   }

   @Override
   public void execute(BlockingRunnable runnable) {
      initIfNeeded();
      delegate.execute(runnable);
   }

   @Override
   public void checkForReadyTasks() {
      if (delegate != null) {
         delegate.checkForReadyTasks();
      }
   }

   @Override
   public void shutdown() {
      if (delegate != null) delegate.shutdown();
   }

   @Override
   public List<Runnable> shutdownNow() {
      if (delegate == null)
         return InfinispanCollections.emptyList();
      else
         return delegate.shutdownNow();
   }

   @Override
   public boolean isShutdown() {
      return delegate == null || delegate.isShutdown();
   }

   @Override
   public boolean isTerminated() {
      return delegate == null || delegate.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      if (delegate == null)
         return true;
      else
         return delegate.awaitTermination(timeout, unit);
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      initIfNeeded();
      return delegate.submit(task);
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      initIfNeeded();
      return delegate.submit(task, result);
   }

   @Override
   public Future<?> submit(Runnable task) {
      initIfNeeded();
      return delegate.submit(task);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      initIfNeeded();
      return delegate.invokeAll(tasks);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      initIfNeeded();
      return delegate.invokeAll(tasks, timeout, unit);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      initIfNeeded();
      return delegate.invokeAny(tasks);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      initIfNeeded();
      return delegate.invokeAny(tasks, timeout, unit);
   }

   @Override
   public void execute(Runnable command) {
      initIfNeeded();
      delegate.execute(command);
   }

   private void initIfNeeded() {
      if (delegate == null) {
         synchronized (this) {
            if (delegate == null) {
               delegate = new BlockingTaskAwareExecutorServiceImpl(controllerThreadName ,
                                                                   executorFactory.createExecutor(threadFactory),
                                                                   timeService);
            }
         }
      }
   }
}
