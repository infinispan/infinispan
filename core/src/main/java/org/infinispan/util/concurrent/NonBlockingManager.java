package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * TODO:
 */
public interface NonBlockingManager {
   /**
    * TODO:
    * @param supplier
    * @param initialDelay
    * @param delay
    * @param unit
    * @return
    */
   AutoCloseable scheduleWithFixedDelay(Supplier<CompletionStage<?>> supplier, long initialDelay, long delay, TimeUnit unit);
}
