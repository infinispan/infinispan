package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;

/**
 * A tracker for synchronous requests.
 */
public interface RequestTracker {

   /**
    * The terminal outcome of a tracked request. Every request resolves to exactly one of these.
    */
   enum Outcome {

      /**
       * A reply was received within the request timeout.
       */
      SUCCESS,

      /**
       * The request timed out.
       */
      TIMEOUT,

      /**
       * The request was abandoned before a definitive outcome (e.g. an early multi-target completion, a leaver, or
       *  a cancellation).
       */
      ABANDONED
   }

   /**
    * @return The destination {@link Address} of the request. It is never {@code null}.
    */
   Address destination();

   /**
    * Resets the send time to the current time.
    */
   // for staggered sends
   void resetSendTime();

   /**
    * Resolves this request with its terminal {@code outcome}, freeing the in-flight slot held for the destination.
    *
    * <p>
    * Idempotent: the first call wins and any subsequent call is ignored, so a request may be resolved defensively (for
    * example by a completion callback) after it has already recorded a {@link Outcome#SUCCESS} or {@link Outcome#TIMEOUT}
    * without double-counting or changing the final outcome.
    * </p>
    *
    * @param outcome how the request ended; determines what, if anything, is recorded for the destination.
    */
   void resolve(Outcome outcome);

   /**
    * Returns whether a new request to this destination should be failed immediately instead of sent.
    *
    * <p>
    * Unlike {@link #adjustTimeout(long)}, which only shortens how long an already-sent request waits, this allows
    * the caller to avoid the round-trip (and the destination's own processing cost) entirely once the destination
    * is backlogged enough that sending would almost certainly be wasted work.
    * </p>
    *
    * @return {@code true} if the request should be failed without being sent
    */
   boolean shouldShed();

   /**
    * Returns the bucket-adjusted timeout for this destination.
    *
    * @param timeoutNanos the caller-provided RPC timeout in nanoseconds
    * @return the effective timeout in nanoseconds, shortened for degraded destinations
    */
   long adjustTimeout(long timeoutNanos);
}
