package org.infinispan.util.concurrent;

import java.util.concurrent.CompletableFuture;
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
    * Completes the provided future with the given value. If the future does not have any dependents it will complete
    * it in the invoking thread. However, if there are any dependents it will complete it in a non blocking thread.
    * This is a best effort to prevent a context switch for a stage that does not yet have a dependent while also
    * handing off the dependent processing to a non blocking thread if necessary.
    * @param future the future to complete
    * @param value the value to complete the future with
    * @param <T> the type of the value
    */
   <T> void complete(CompletableFuture<? super T> future, T value);
}
