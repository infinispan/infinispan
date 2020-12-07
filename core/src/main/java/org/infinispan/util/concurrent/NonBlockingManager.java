package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Manager utility for non-blocking operations.
 */
public interface NonBlockingManager {
   /**
    * Schedules the supplier that is executed after the <b>initialDelay</b> period and subsequently runs <b>delay</b>
    * after the previous stage completes. The supplier must not block the thread in which it runs and should immediately
    * return to avoid blocking the scheduling thread.
    * @param supplier non-blocking operation supplier.
    * @param initialDelay period of time before the supplier is invoked.
    * @param delay delay between subsequent supplier invocations.
    * @param unit time unit for delays.
    * @return an AutoCloseable that cancels the scheduled task.
    */
   AutoCloseable scheduleWithFixedDelay(Supplier<CompletionStage<?>> supplier, long initialDelay, long delay, TimeUnit unit);

   /**
    * The returned stage when chained will perform any of those tasks on the same executor as the
    * invoking thread. If the provided stage is already complete the stage will be unchanged.
    * @param stage stage to make sure it is chained on the same executor that is currently running
    * @param <V> the return value of the stage
    * @return resulting stage that when chained will run those operations on the same executor
    */
   default <V> CompletionStage<V> resumeOnSameExecutor(CompletionStage<V> stage) {
      return stage;
   }
}
