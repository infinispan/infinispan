package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Manager to help assist with operations that are required to be non blocking that isn't as simple as just using
 * an executor.
 */
public interface NonBlockingManager {
   /**
    * Schedules the supplier to be executed initial after <b>initialDelay</b> and subsequently will be ran <b>delay</b>
    * after the previous stage completes. This supplier must not block and should immediately return to not block the
    * scheduling thread.
    * @param supplier non blocking operation supplier
    * @param initialDelay initial delay before the supplier is invoked
    * @param delay delay between subsequent supplier invocations
    * @param unit the time unit for the delays
    * @return an AutoCloseable that can be used to cancel the scheduled task from running anymore
    */
   AutoCloseable scheduleWithFixedDelay(Supplier<CompletionStage<?>> supplier, long initialDelay, long delay, TimeUnit unit);
}
