package org.infinispan.executors;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exectues given tasks in provided executor.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ExecutorAllCompletionService implements CompletionService<Void> {
   private Executor executor;
   private boolean interrupted = false;
   private boolean exceptionThrown = false;
   private AtomicLong scheduled = new AtomicLong();
   private AtomicLong completed = new AtomicLong();

   public ExecutorAllCompletionService(Executor executor) {
      this.executor = executor;
   }

   @Override
   public Future<Void> submit(final Callable<Void> task) {
      final FutureImpl future = new FutureImpl();
      scheduled.incrementAndGet();
      executor.execute(new Runnable() {
         @Override
         public void run() {
            try {
               task.call();
            } catch (Exception e) {
               exceptionThrown = true;
               future.exception = e;
            } finally {
               synchronized (future) {
                  future.completed = true;
                  future.notifyAll();
               }
               long count = completed.incrementAndGet();
               if (count == scheduled.get()) {
                  synchronized (ExecutorAllCompletionService.this) {
                     ExecutorAllCompletionService.this.notifyAll();
                  }
               }
            }
         }
      });
      return future;
   }

   @Override
   public Future<Void> submit(final Runnable task, Void result) {
      if (result != null) throw new IllegalArgumentException();
      final FutureImpl future = new FutureImpl();
      scheduled.incrementAndGet();
      executor.execute(new Runnable() {
         @Override
         public void run() {
            try {
               task.run();
            } catch (Throwable e) {
               exceptionThrown = true;
               future.exception = e;
            } finally {
               synchronized (future) {
                  future.completed = true;
                  future.notifyAll();
               }
               long count = completed.incrementAndGet();
               if (count == scheduled.get()) {
                  synchronized (ExecutorAllCompletionService.this) {
                     ExecutorAllCompletionService.this.notifyAll();
                  }
               }
            }
         }
      });
      return future;
   }

   /**
    * @return True if all currently scheduled tasks have already been completed, false otherwise;
    */
   public boolean isAllCompleted() {
      return completed.get() >= scheduled.get();
   }

   public void waitUntilAllCompleted() {
      while (completed.get() < scheduled.get()) {
         synchronized (this) {
            try {
               wait(100);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      }
   }

   public boolean isExceptionThrown() {
      return exceptionThrown;
   }

   @Override
   public Future<Void> take() throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public Future<Void> poll() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Future<Void> poll(long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   private class FutureImpl implements Future<Void> {
      volatile boolean completed = false;
      Throwable exception = null;

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean isCancelled() {
         return false;
      }

      @Override
      public boolean isDone() {
         return completed;
      }

      @Override
      public Void get() throws InterruptedException, ExecutionException {
         synchronized (this) {
            while (!completed) wait();
            if (exception != null) {
               throw new ExecutionException(exception);
            }
         }
         return null;
      }

      @Override
      public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         synchronized (this) {
            while (!completed) wait(unit.toMillis(timeout));
            if (exception != null) {
               throw new ExecutionException(exception);
            }
         }
         return null;
      }
   }
}
