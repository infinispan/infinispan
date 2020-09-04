package org.infinispan.util;

import org.infinispan.commons.util.Experimental;

/**
 * Interface to implement an exponential back-off algorithm that retries the request based on the result of the remote
 * operation.
 * <p>
 * This interface contains 2 methods: {@link #backoffSleep()} which should be invoked if the request needs to be retried
 * and {@link #reset()}, invoked when a request is "successful", which resets the state.
 * <p>
 * The interface may me changed in the future to include async methods.
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
      public void backoffSleep() {
         //no-op
      }

      @Override
      public void reset() {
         //no-op
      }
   };

   /**
    * It blocks the thread for a certain amount of time before retries the request.
    * <p>
    * The method is blocking and should be invoked when a request needs to be retried. It blocks the thread for a
    * certain amount of time until it is allowed to do the request again.
    *
    * @throws InterruptedException If the {@link Thread} is interrupted while blocked.
    */
   void backoffSleep() throws InterruptedException;

   /**
    * Resets its state.
    * <p>
    * The blocking time in {@link #backoffSleep()} increases with the number of consecutive retries. This methods resets
    * its state back to the initial sleep time.
    */
   void reset();

}
