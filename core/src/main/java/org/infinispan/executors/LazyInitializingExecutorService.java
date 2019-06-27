package org.infinispan.executors;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.concurrent.WithinThreadExecutor;

/**
 * A delegating executor that lazily constructs and initializes the underlying executor, since unused JDK executors
 * are expensive.
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Scope(Scopes.GLOBAL)
public final class LazyInitializingExecutorService extends ManageableExecutorService<ExecutorService> implements ExecutorService {
   private static final ExecutorService STOPPED;

   static {
      STOPPED = new WithinThreadExecutor();
      STOPPED.shutdown();
   }

   private final ThreadPoolExecutorFactory<ExecutorService> executorFactory;
   private final ThreadFactory threadFactory;

   public LazyInitializingExecutorService(
         ThreadPoolExecutorFactory<ExecutorService> executorFactory, ThreadFactory threadFactory) {
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
      synchronized (this) {
         if (executor == null) {
            executor = STOPPED;
         }
         executor.shutdown();
      }
   }

   @Stop
   @Override
   public List<Runnable> shutdownNow() {
      synchronized (this) {
         if (executor == null) {
            executor = STOPPED;
         }
         return executor.shutdownNow();
      }
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
}
