package org.infinispan.executors;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CompletableExecutorService {
   private final ExecutorService executorService;

   public CompletableExecutorService(ExecutorService executorService) {
      this.executorService = executorService;
   }

   public void shutdown() {
      executorService.shutdown();
   }

   public List<Runnable> shutdownNow() {
      return executorService.shutdownNow();
   }

   public boolean isShutdown() {
      return executorService.isShutdown();
   }

   public boolean isTerminated() {
      return executorService.isTerminated();
   }

   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return executorService.awaitTermination(timeout, unit);
   }

   public <T> CompletableFuture<T> submit(Callable<T> task) {
      CompletableFuture<T> cf = new CompletableFuture<>();
      executorService.execute(() -> {
         try {
            cf.complete(task.call());
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
      });
      return cf;
   }

   public CompletableFuture<?> submit(Runnable task) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      executorService.execute(() -> {
         try {
            task.run();
            cf.complete(null);
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
      });
      return cf;
   }
}
