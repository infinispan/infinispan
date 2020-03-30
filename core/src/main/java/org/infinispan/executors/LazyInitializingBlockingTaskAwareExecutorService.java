package org.infinispan.executors;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;

/**
 * A delegating executor that lazily constructs and initializes the underlying executor.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Scope(Scopes.GLOBAL)
public final class LazyInitializingBlockingTaskAwareExecutorService
   extends ManageableExecutorService<ExecutorService> implements BlockingTaskAwareExecutorService {

   private static final BlockingTaskAwareExecutorService STOPPED;

   static {
      STOPPED = new EmptyBlockingTaskAwareExecutorService();
   }

   static final class EmptyBlockingTaskAwareExecutorService extends AbstractExecutorService implements BlockingTaskAwareExecutorService {

      @Override
      public void execute(BlockingRunnable runnable) {
         throw new RejectedExecutionException();
      }

      @Override
      public void checkForReadyTasks() {

      }

      @Override
      public void shutdown() {

      }

      @Override
      public List<Runnable> shutdownNow() {
         return Collections.emptyList();
      }

      @Override
      public boolean isShutdown() {
         return true;
      }

      @Override
      public boolean isTerminated() {
         return true;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         return true;
      }

      @Override
      public void execute(Runnable command) {
         throw new RejectedExecutionException();
      }
   }

   private final ThreadPoolExecutorFactory<ExecutorService> executorFactory;
   private final ThreadFactory threadFactory;
   private final TimeService timeService;
   private volatile BlockingTaskAwareExecutorService blockingExecutor;

   public LazyInitializingBlockingTaskAwareExecutorService(ThreadPoolExecutorFactory<ExecutorService> executorFactory,
                                                           ThreadFactory threadFactory,
                                                           TimeService timeService) {
      this.executorFactory = executorFactory;
      this.threadFactory = threadFactory;
      this.timeService = timeService;
   }

   @Override
   public void execute(BlockingRunnable runnable) {
      initIfNeeded();
      blockingExecutor.execute(runnable);
   }

   @Override
   public void checkForReadyTasks() {
      if (blockingExecutor != null) {
         blockingExecutor.checkForReadyTasks();
      }
   }

   @Override
   public void shutdown() {
      synchronized (this) {
         if (blockingExecutor == null) {
            blockingExecutor = STOPPED;
         }
         blockingExecutor.shutdown();
      }
   }

   @Stop
   @Override
   public List<Runnable> shutdownNow() {
      synchronized (this) {
         if (blockingExecutor == null) {
            blockingExecutor = STOPPED;
         }
         return blockingExecutor.shutdownNow();
      }
   }

   @Override
   public boolean isShutdown() {
      return blockingExecutor == null || blockingExecutor.isShutdown();
   }

   @Override
   public boolean isTerminated() {
      return blockingExecutor == null || blockingExecutor.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      if (blockingExecutor == null)
         return true;
      else
         return blockingExecutor.awaitTermination(timeout, unit);
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      initIfNeeded();
      return blockingExecutor.submit(task);
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      initIfNeeded();
      return blockingExecutor.submit(task, result);
   }

   @Override
   public Future<?> submit(Runnable task) {
      initIfNeeded();
      return blockingExecutor.submit(task);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      initIfNeeded();
      return blockingExecutor.invokeAll(tasks);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      initIfNeeded();
      return blockingExecutor.invokeAll(tasks, timeout, unit);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      initIfNeeded();
      return blockingExecutor.invokeAny(tasks);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      initIfNeeded();
      return blockingExecutor.invokeAny(tasks, timeout, unit);
   }

   @Override
   public void execute(Runnable command) {
      initIfNeeded();
      blockingExecutor.execute(command);
   }

   public BlockingTaskAwareExecutorService getExecutorService() {
      return blockingExecutor;
   }

   private void initIfNeeded() {
      if (blockingExecutor == null) {
         synchronized (this) {
            if (blockingExecutor == null) {
               // The superclass methods only work if the blockingExecutor is a ThreadPoolExecutor
               this.executor = executorFactory.createExecutor(threadFactory);
               this.blockingExecutor =
                  new BlockingTaskAwareExecutorServiceImpl(executor, timeService);
            }
         }
      }
   }
}
