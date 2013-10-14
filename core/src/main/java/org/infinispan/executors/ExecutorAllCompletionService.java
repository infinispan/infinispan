package org.infinispan.executors;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exectues given tasks in provided executor.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ExecutorAllCompletionService implements CompletionService<Void> {
   private ExecutorCompletionService executorService;
   private AtomicReference<ExecutionException> firstException = new AtomicReference<ExecutionException>();
   private AtomicLong scheduled = new AtomicLong();
   private AtomicLong completed = new AtomicLong();

   public ExecutorAllCompletionService(Executor executor) {
      this.executorService = new ExecutorCompletionService(executor);
   }

   @Override
   public Future<Void> submit(final Callable<Void> task) {
      scheduled.incrementAndGet();
      Future<Void> future = executorService.submit(task);
      pollUntilEmpty();
      return future;
   }

   @Override
   public Future<Void> submit(final Runnable task, Void result) {
      scheduled.incrementAndGet();
      Future<Void> future = executorService.submit(task, result);
      pollUntilEmpty();
      return future;
   }

   private void pollUntilEmpty() {
      Future<Void> completedFuture;
      while ((completedFuture = executorService.poll()) != null) {
         try {
            completedFuture.get();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (ExecutionException e) {
            if (firstException.get() == null) {
               firstException.compareAndSet(null, e);
            }
         } finally {
            completed.incrementAndGet();
         }
      }
   }

   /**
    * @return True if all currently scheduled tasks have already been completed, false otherwise;
    */
   public boolean isAllCompleted() {
      return completed.get() >= scheduled.get();
   }

   public void waitUntilAllCompleted() {
      while (completed.get() < scheduled.get()) {
         // Here is a race - if we poll the last scheduled entry elsewhere, we may wait
         // another 100 ms until we realize that everything has already completed.
         // Nevertheless, that's not so bad.
         try {
            Future<Void> future = poll(100, TimeUnit.MILLISECONDS);
            if (future != null) {
               future.get();
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
         } catch (ExecutionException e) {
            if (firstException.get() == null) {
               firstException.compareAndSet(null, e);
            }
         }
      }
   }

   public boolean isExceptionThrown() {
      return firstException.get() != null;
   }

   public ExecutionException getFirstException() {
      return firstException.get();
   }

   @Override
   public Future<Void> take() throws InterruptedException {
      Future<Void> future = executorService.take();
      completed.incrementAndGet();
      return future;
   }

   @Override
   public Future<Void> poll() {
      Future<Void> future = executorService.poll();
      if (future != null) {
         completed.incrementAndGet();
      }
      return future;
   }

   @Override
   public Future<Void> poll(long timeout, TimeUnit unit) throws InterruptedException {
      Future<Void> future = executorService.poll(timeout, unit);
      if (future != null) {
         completed.incrementAndGet();
      }
      return future;
   }
}
