package org.infinispan.interceptors.distribution;

import java.util.concurrent.CompletableFuture;

/**
 * Represents the ack collector for a write operation in triangle algorithm.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface Collector<T> {

   /**
    * @return The {@link CompletableFuture} that will be completed when all the acks are received.
    */
   CompletableFuture<T> getFuture();

   /**
    * The exception results of the primary owner.
    *
    * @param throwable the {@link Throwable} throw by the primary owner
    */
   void primaryException(Throwable throwable);

   /**
    * The write operation's return value.
    *
    * @param result  the operation's return value
    * @param success {@code true} if it was successful, {@code false} otherwise (for conditional operations).
    */
   void primaryResult(T result, boolean success);

}
