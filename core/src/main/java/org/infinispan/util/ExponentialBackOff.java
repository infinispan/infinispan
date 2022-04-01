package org.infinispan.util;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Interface to implement an exponential back-off algorithm that retries the request based on the result of the remote
 * operation.
 * <p>
 * This interface contains 2 methods: {@link #asyncBackOff()} ()} which should be invoked if the request needs to be
 * retried and {@link #reset()}, invoked when a request is "successful", which resets the state.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Experimental
public interface ExponentialBackOff {

   /**
    * Disabled exponential back-off algorithm. It does nothing.
    */
   ExponentialBackOff NO_OP = new ExponentialBackOff() {

      @Override
      public void reset() {
         //no-op
      }

      @Override
      public CompletionStage<Void> asyncBackOff() {
         return CompletableFutures.completedNull();
      }
   };

   /**
    * Resets its state.
    * <p>
    * The blocking time in {@link #asyncBackOff()} increases with the number of consecutive retries. This methods resets
    * its state back to the initial sleep time.
    */
   void reset();

   /**
    * It returns a {@link CompletionStage} which is completed a certain amount of time before retries the request.
    * <p>
    * After the completion, the request is allows to proceed.
    *
    * @return A {@link CompletionStage}.
    */
   CompletionStage<Void> asyncBackOff();

}
