package org.infinispan.xsite.irac;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Executes the "IRAC" sending task in a single thread.
 * <p>
 * This executor makes sure no more than one task is running at the same time. Also, it avoids "queueing" multiple tasks
 * by queuing at most one. This is possible because the task does the same thing: iterator over pending updates and send
 * them to the remote site.
 * <p>
 * In addition, it iteracts with the {@link ExponentialBackOff} to add delays in case of network failures.
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracExecutor implements Function<Void, CompletionStage<Void>> {

   private static final Log log = LogFactory.getLog(IracExecutor.class);

   private final WrappedRunnable runnable;
   private volatile CompletableFuture<Void> lastRunnable;
   private volatile Executor executor;
   private volatile ExponentialBackOff backOff;
   final AtomicBoolean hasPendingRunnable;
   private volatile boolean backOffEnabled;


   public IracExecutor(Supplier<CompletionStage<Void>> runnable) {
      this.runnable = new WrappedRunnable(Objects.requireNonNull(runnable));
      this.lastRunnable = CompletableFutures.completedNull();
      this.executor = new WithinThreadExecutor();
      this.hasPendingRunnable = new AtomicBoolean();
   }

   public void setExecutor(Executor executor) {
      this.executor = Objects.requireNonNull(executor);
   }

   public void setBackOff(ExponentialBackOff backOff) {
      this.backOff = Objects.requireNonNull(backOff);
   }

   /**
    * Executes, in a new thread, or queues the task.
    */
   public void run() {
      if (hasPendingRunnable.compareAndSet(false, true)) {
         if (backOffEnabled) {
            //noinspection NonAtomicOperationOnVolatileField
            lastRunnable = lastRunnable.thenCompose(this).thenComposeAsync(runnable, executor);
         } else {
            //noinspection NonAtomicOperationOnVolatileField
            lastRunnable = lastRunnable.thenComposeAsync(runnable, executor);
         }
      }
   }

   public void enableBackOff() {
      backOffEnabled = true;
   }

   public void disableBackOff() {
      backOffEnabled = false;
      backOff.reset();
   }

   /**
    * Used by {@link CompletableFuture#thenComposeAsync(Function, Executor)}, it adds the {@link ExponentialBackOff}
    * delay.
    *
    * @param unused Unused value.
    * @return The {@link CompletionStage} from {@link ExponentialBackOff#asyncBackOff()}.
    */
   @Override
   public CompletionStage<Void> apply(Void unused) {
      return backOff.asyncBackOff();
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
            log.trace("Unexpected exception", e);
            return CompletableFutures.completedNull();
         }
      }
   }
}
