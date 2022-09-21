package org.infinispan.xsite.irac;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Executes the "IRAC" sending task in a single thread.
 * <p>
 * This executor makes sure no more than one task is running at the same time. Also, it avoids "queueing" multiple tasks
 * by queuing at most one. This is possible because the task does the same thing: iterator over pending updates and send
 * them to the remote site.
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracExecutor implements Runnable {

   private static final Log log = LogFactory.getLog(IracExecutor.class);

   private final WrappedRunnable runnable;
   private volatile CompletableFuture<Void> lastRunnable;
   private volatile Executor executor;
   final AtomicBoolean hasPendingRunnable;


   public IracExecutor(Supplier<CompletionStage<Void>> runnable) {
      this.runnable = new WrappedRunnable(Objects.requireNonNull(runnable));
      this.lastRunnable = CompletableFutures.completedNull();
      this.executor = new WithinThreadExecutor();
      this.hasPendingRunnable = new AtomicBoolean();
   }

   public void setExecutor(Executor executor) {
      this.executor = Objects.requireNonNull(executor);
   }

   /**
    * Executes, in a new thread, or queues the task.
    */
   @Override
   public void run() {
      if (hasPendingRunnable.compareAndSet(false, true)) {
         //noinspection NonAtomicOperationOnVolatileField
         lastRunnable = lastRunnable.thenComposeAsync(runnable, executor);
      }
   }

   private class WrappedRunnable implements Function<Void, CompletionStage<Void>> {
      private final Supplier<CompletionStage<Void>> runnable;

      private WrappedRunnable(Supplier<CompletionStage<Void>> runnable) {
         this.runnable = runnable;
      }

      /**
       * Use by {@link CompletableFuture#thenComposeAsync(Function, Executor)}, executes the task and returns the {@link
       * CompletionStage} return by it.
       *
       * @param unused Unused value.
       * @return The {@link CompletionStage} from the task.
       */
      @Override
      public CompletionStage<Void> apply(Void unused) {
         hasPendingRunnable.set(false);
         try {
            return runnable.get();
         } catch (Throwable e) {
            log.unexpectedErrorFromIrac(e);
            return CompletableFutures.completedNull();
         }
      }
   }

   public Executor executor() {
      return executor;
   }
}
