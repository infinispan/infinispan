package org.infinispan.remoting.transport.jgroups;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

// Pre-padding
abstract class AdaptiveTimeoutPad1 {
   private int pi1;
   private long p01, p02, p03, p04, p05, p06, p07;
}

// The majority of the state related to one destination
abstract class AdaptiveTimeoutState extends AdaptiveTimeoutPad1 {
   // This field is hidden between two padding layers.
   protected volatile long packedState;
}

// Middle padding separating state from counter.
abstract class AdaptiveTimeoutPad2 extends AdaptiveTimeoutState {
   private long p10, p11, p12, p13, p14, p15, p16, p17;
}

// Pad for the in-flight counter on its own cache line.
abstract class AdaptiveTimeoutInFlight extends AdaptiveTimeoutPad2 {
   // An int here should be enough to track in-flight operations to a single destination.
   // It shouldn't happen to have more than Integer.MAX_VALUE pending operations.
   protected volatile int paddedInFlight;

   // Int padding to fill gap.
   private int pi1;
}

// Middle padding separating in-flight counter from the limit state
abstract class AdaptiveTimeoutPad3 extends AdaptiveTimeoutInFlight {
   private long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class AdaptiveTimeoutVegas extends AdaptiveTimeoutPad3 {
   static final int BASE_RTT_SHIFT = 0;
   static final int LIMIT_SHIFT = 32;
   static final long INT_MASK = 0xFFFFFFFFL;
   static final long MAX_CONCURRENT_REQUESTS = 1 << 16;

   // Bits 0-31  (32 bits): baseRttUs in kibi-nanoseconds (units of 1024, max ~1.2 hours)
   // Bits 32-63 (32 bits): concurrencyLimit (Max ~2 billion)
   protected volatile long packedLimit;
}

/**
 * Per-destination adaptive timeout that modulates the caller-provided RPC timeout based on the target node's recent behavior.
 *
 * <p>
 * Tracks round-trip time using exponentially weighted moving averages (EMA) for both smoothed RTT and RTT variance,
 * following the TCP-style estimation from RFC 6298. A token bucket controls how aggressively the timeout is shortened for
 * degraded nodes: consecutive timeouts drain the bucket, successful RPCs refill it fractionally.
 * </p>
 *
 * <p>
 * When the bucket is full, the configured timeout is used unchanged. As the bucket drains, the effective timeout interpolates
 * down toward an adaptive floor derived from observed healthy latency ({@code srtt + k * rttvar}). When the bucket is
 * near-empty, periodic probe requests use the full configured timeout so that recovering nodes can earn credit back.
 * </p>
 *
 * @since 16.3
 * @author José Bolina
 * @see <a href="https://www.rfc-editor.org/info/rfc6298/">RFC-6298: Computing TCP's Retransmission Timer</a>
 */
public final class AdaptiveTimeout extends AdaptiveTimeoutVegas {

   // Values suggested by the RFC, §2.3.
   // The alpha is 1 / 8, i.e., shift 3. Beta is 1/4, i.e., shift 2.
   // Extracted from Jacobson, V. and M. Karels, "Congestion Avoidance and Control", ftp://ftp.ee.lbl.gov/papers/congavoid.ps.Z.
   private static final int SRTT_ALPHA_SHIFT = 3;
   private static final int RTTVAR_BETA_SHIFT = 2;

   // Per RFC at §2.2, K = 4, shift by 2 to multiply by 4.
   private static final int FLOOR_K_SHIFT = 2;

   // We pack the whole state into a single long variable.
   // This allows us to implement some tricks better for concurrency.
   // The layout is as follows:
   // Bits 00-27 (28 bits): SRTT in kibi-nanoseconds (units of 1024 ns, max ~274 seconds).
   // Bits 28-55 (28 bits): RTTVAR in kibi-nanoseconds (units of 1024 ns, max ~274 seconds).
   // Bits 56-59 (4 bits) : Bucket level, 0 to 8 levels. Timeout costs 2, success deposits 1.
   // Bits 60-62 (3 bits) : Probe counter, 0 to 7. Counter to allow a generous request.
   // Bit  63    (1 bit)  : Initialized flag.
   private static final long LATENCY_MASK = 0x0FFFFFFFL;
   private static final int SRTT_SHIFT = 0;
   private static final int RTTVAR_SHIFT = 28;

   // Asymmetric bucket.
   // Drains in 4 timeouts and refills in 8 successes.
   // It provides a fast degradation and slow recovery.
   private static final long BUCKET_MASK = 0x0FL;
   private static final int BUCKET_SHIFT = 56;
   private static final long MAX_BUCKET_LEVEL = 8L;
   private static final long BUCKET_TIMEOUT_COST = 2L;
   private static final long BUCKET_SUCCESS_DEP = 1L;

   // Provide one generous request to probe whether the node has recovered.
   // For example, a degraded node provided credit allows for 50ms, because the node used to reply in that time.
   // The node has recovered, but for some reason it is yet not capable of handling at 50ms, only at 100ms.
   // We allow one generous request after some small retries to give the node the benefit of the doubt to recover credits.
   private static final long PROBE_THRESHOLD = 2L;
   private static final long PROBE_MASK = 0x07L;
   private static final int PROBE_SHIFT = 60;

   private static final long INIT_BIT = 1L << 63;

   private static final VarHandle STATE_HANDLE;
   private static final VarHandle IN_FLIGHT_HANDLE;
   private static final VarHandle LIMIT_HANDLE;

   // Additional padding
   private long p10, p11, p12, p13, p14, p15, p16, p17;

   static {
      try {
         MethodHandles.Lookup l = MethodHandles.lookup();
         STATE_HANDLE = l.findVarHandle(AdaptiveTimeout.class, "packedState", long.class);
         IN_FLIGHT_HANDLE = l.findVarHandle(AdaptiveTimeout.class, "paddedInFlight", int.class);
         LIMIT_HANDLE = l.findVarHandle(AdaptiveTimeout.class, "packedLimit", long.class);
      } catch (Exception e) {
         throw new ExceptionInInitializerError(e);
      }
   }

   public AdaptiveTimeout() {
      this.packedState = MAX_BUCKET_LEVEL << BUCKET_SHIFT;
      this.packedLimit = MAX_CONCURRENT_REQUESTS << LIMIT_SHIFT;
   }

   /**
    * Increments the in-flight request counter for this destination.
    */
   public void incrementInFlight() {
      IN_FLIGHT_HANDLE.getAndAdd(this, 1);
   }

   /**
    * Decrements the in-flight request counter for this destination.
    */
   public void removeInFlight() {
      int prev = (int) IN_FLIGHT_HANDLE.getAndAdd(this, -1);
      assert prev > 0 : "In flight requests is bellow 0";
   }

   /**
    * @return {@code true} if this destination is backlogged enough that a new request should be failed immediately
    * instead of sent, based on the current number of in-flight requests.
    */
   public boolean shouldShed() {
      return this.paddedInFlight >= limitOf(this.packedLimit);
   }

   /**
    * Records a successful RPC and updates the latency estimates.
    *
    * <p>
    * On the first sample, SRTT is set directly to the observed latency and variance is set to half the latency (RFC 6298
    * initialization). Subsequent samples apply exponential smoothing. Each success also deposits a fractional token into
    * the node's bucket.
    * </p>
    *
    * @param latencyNs observed round-trip latency in nanoseconds
    */
   public void recordSuccess(long latencyNs) {
      assert latencyNs >= 0 : "Latency should be equal or greater than 0ns";
      long latencyKibiNs = Math.min(latencyNs >>> 10, LATENCY_MASK);
      long current, witness, next;

      long newBucket;
      long newSrtt;

      do {
         current = packedState;
         boolean initialized = isInitialized(current);
         long bucket = bucketOf(current);
         long probe = probeOf(current);

         newBucket = Math.min(bucket + BUCKET_SUCCESS_DEP, MAX_BUCKET_LEVEL);
         long newRttVar;

         if (!initialized) {
            newSrtt = latencyKibiNs;
            // RTTVAR initialized (latency / 2). §2.2.
            newRttVar = latencyKibiNs >> 1;
            initialized = true;
         } else {
            long srtt = srttOf(current);
            long rttvar = rttvarOf(current);

            // RFC 6298 EMA utilizing only bitshift to avoid float number operations.
            // This follows the calculation provided at RFC §2.3.
            // RTTVAR = (1 - 1/4) * RTTVAR + 1/4 * |SRTT - Sample|
            long diff = Math.abs(latencyKibiNs - srtt);
            newRttVar = rttvar - (rttvar >> RTTVAR_BETA_SHIFT) + (diff >> RTTVAR_BETA_SHIFT);

            // SRTT = (1 - 1/8) * SRTT + 1/8 * Sample
            newSrtt = srtt - (srtt >> SRTT_ALPHA_SHIFT) + (latencyKibiNs >> SRTT_ALPHA_SHIFT);

            newRttVar = Math.min(newRttVar, LATENCY_MASK);
            newSrtt = Math.min(newSrtt, LATENCY_MASK);
         }

         next = pack(newSrtt, newRttVar, newBucket, probe, initialized);
         witness = (long) STATE_HANDLE.compareAndExchange(this, current, next);
      } while (witness != current);

      // Update the gradient
      updateLimit(latencyKibiNs, newSrtt, newBucket, false);
   }

   /**
    * Records a timed-out RPC by draining one token from the bucket.
    *
    * <p>
    * Does not update the latency estimates. A timeout gives no information about actual round-trip time, only that it
    * exceeded the deadline.
    * </p>
    */
   public void recordTimeout() {
      long current, witness, next;
      long newBucket;
      do {
         current = packedState;
         long bucket = bucketOf(current);

         // The bucket is already drained, we don't need to update anything.
         if (bucket == 0)
            return;

         newBucket = Math.max(0, bucket - BUCKET_TIMEOUT_COST);

         long srtt = srttOf(current);
         long rttvar = rttvarOf(current);
         long probe = probeOf(current);
         boolean initialized = isInitialized(current);

         next = pack(srtt, rttvar, newBucket, probe, initialized);
         witness = (long) STATE_HANDLE.compareAndExchange(this, current, next);
      } while (witness != current);

      // Reactive Update: Bucket has drained. Pass 0 for latencies.
      // updateLimit will instantly halve the concurrency limit.
      updateLimit(0, 0, newBucket, true);
   }

   /**
    * Returns the effective timeout for this destination.
    *
    * <p>
    * Interpolates between the adaptive floor and the configured timeout based on the bucket fill ratio. A full bucket
    * returns the configured timeout unchanged; an empty bucket returns the adaptive floor. When the bucket is near-empty,
    * periodically returns the configured timeout as a probe to test if the node has recovered.
    * </p>
    *
    * @param configuredTimeoutNs the caller-provided RPC timeout in nanoseconds
    * @return the bucket-adjusted timeout in nanoseconds
    */
   public long adjustTimeout(long configuredTimeoutNs) {
      if (configuredTimeoutNs <= 0)
         return 0;

      long current = packedState;

      if (!isInitialized(current))
         return configuredTimeoutNs;

      long bucket = bucketOf(current);
      long srttKibiNs = srttOf(current);
      long rttvarKibiNs = rttvarOf(current);
      // Transform the passed timeout into kibi, too.
      long configuredTimeoutKibiNs = configuredTimeoutNs >>> 10;
      long boundedFloorNs = calculateBoundedFloorNs(srttKibiNs, rttvarKibiNs, configuredTimeoutKibiNs);

      // Initial calculation interpolating on the bucket measurements.
      // We start with the provided timeout value and adjust by interpolating with the bucket value.
      long effective = configuredTimeoutNs;

      // Degraded state handling.
      // Write only ever happens when one of the nodes is in a degraded state.
      // Otherwise, it will proceed directly to read the values and calculate the timeout.
      if (bucket < PROBE_THRESHOLD) {
         long probe = probeOf(current);
         long nextProbe = (probe + 1) & PROBE_MASK;

         // No do-while loop here.
         // Since the probe request is just a best effort to give some room for the degraded node.
         // If we are updating it concurrently, at least one update will succeed, and that one update might use a full timeout.
         long next = pack(srttKibiNs, rttvarKibiNs, bucket, nextProbe, true);
         long witness = (long) STATE_HANDLE.compareAndExchange(this, current, next);

         // If this request is not a probe trying to recover the destination, interpolate down
         // If it is a probe, effective remains as the configured timeout.
         if (witness != current || nextProbe != 0) {
               effective = boundedFloorNs + (((configuredTimeoutNs - boundedFloorNs) * bucket) >> 3);
         } else {
            return effective;
         }
      } else {
         effective = boundedFloorNs + (((configuredTimeoutNs - boundedFloorNs) * bucket) >> 3);
      }

      // Second part in the adjustments to catch stalls with in-flight requests.
      // We skip the current adjusting request when counting the in-flight requests.
      int currentInFlight = this.paddedInFlight - 1;
      if (currentInFlight > 0 && srttKibiNs > 0) {
         // Calculate the accumulated time debt of pending operations in micros to avoid overflows.
         long timeDebtKibiNs = (long) currentInFlight * srttKibiNs;

         if (timeDebtKibiNs >= configuredTimeoutKibiNs) {
            // The queue has pilled enough work that srtt debt exceeds the timeout.
            // Very likely the destination is struggling, let's trip the circuit breaker.
            effective = boundedFloorNs;
         } else {
            // The request still has a chance to succeed, but let's adjust it.
            // Little's law to calculate the adjustment, trying to avoid float-point calculations.
            long diffKibiNs = (effective - boundedFloorNs) >>> 10;

            // Safe: timeDebtUs < configuredTimeoutUs, diffUs * timeDebtUs should remain below Long.MAX_VALUE.
            long penaltyKibiNs = (diffKibiNs * timeDebtKibiNs) / configuredTimeoutKibiNs;
            long reducedNs = effective - (penaltyKibiNs << 10);
            effective = Math.max(reducedNs, boundedFloorNs);
         }
      }

      return effective;
   }

   private void updateLimit(long latencyKibiNs, long srttKibiNs, long bucket, boolean failure) {
      long current, next;
      do {
         current = this.packedLimit;
         long baseRttKibiNs = baseRttOf(current);
         long limit = limitOf(current);

         long newBaseRttKibiNs;
         if (baseRttKibiNs == 0 || latencyKibiNs < baseRttKibiNs) {
            // Guard against a completion of <1us.
            newBaseRttKibiNs = Math.max(1, latencyKibiNs);
         } else {
            // Slowly increases latency to forget old values.
            newBaseRttKibiNs = baseRttKibiNs + 1;
         }

         long newLimit;
         if (failure && bucket < MAX_BUCKET_LEVEL) {
            // REACTIVE: The bucket detected a timeout/partition.
            // Abandon gradient and shrink the limit multiplicatively.
            newLimit = Math.max(128L, limit / 2);
         } else {
            // PROACTIVE: Vegas Gradient.
            // If SRTT balloons away from BaseRTT, (BaseRTT / SRTT) < 1, shrinking the limit.
            // The +4 allows additive growth when healthy.
            long gradientLimit = (limit * newBaseRttKibiNs) / Math.max(1, srttKibiNs);
            long safeFloor = Math.max(128L, gradientLimit + 4);
            newLimit = Math.min(safeFloor, MAX_CONCURRENT_REQUESTS);
         }

         next = (newBaseRttKibiNs & INT_MASK) | (newLimit & INT_MASK) << LIMIT_SHIFT;
      } while ((long) LIMIT_HANDLE.compareAndExchange(this, current, next) != current);
   }

   private static long calculateBoundedFloorNs(long srttKibiNs, long rttvarKibiNs, long configuredTimeoutKibiNs) {
      // Adaptive floor = SRTT + K * RTTVAR
      long floorKibiNs = srttKibiNs + (rttvarKibiNs << FLOOR_K_SHIFT);
      // Bounded floor shrinks harmonically once the raw floor exceeds the provided timeout value.
      long boundedFloorKibiNs;
      if (floorKibiNs < configuredTimeoutKibiNs) {
         boundedFloorKibiNs = floorKibiNs;
      } else if (configuredTimeoutKibiNs == 0) {
         boundedFloorKibiNs = 0;
      } else {
         long clamped = Math.min(configuredTimeoutKibiNs, LATENCY_MASK);
         boundedFloorKibiNs = (clamped * clamped) / floorKibiNs;
      }
      // Observe this converts back from the KibiNs in recordSuccess() instead of division/multiplying by 1_000.
      return boundedFloorKibiNs << 10;
   }

   /**
    * @return the smoothed round-trip time estimate in nanoseconds
    */
   public double srtt() {
      // Stored in kibi-nanoseconds, shift back.
      return srttOf(this.packedState) << 10;
   }

   /**
    * @return the round-trip time variance estimate in nanoseconds
    */
   public double rttvar() {
      // Stored in kibi-nanoseconds, shift back.
      return rttvarOf(this.packedState) << 10;
   }

   /**
    * @return the current token bucket level, between 0 and the bucket capacity.
    */
   public double bucketLevel() {
      return bucketOf(this.packedState) / 2.0;
   }

   /**
    * @return the current number of in-flight operations.
    */
   public int inFlight() {
      return paddedInFlight;
   }

   /**
    * Returns the current adaptive concurrency limit for this destination.
    *
    * <p>
    * A request is shed once the number of in-flight requests reaches this limit. The limit shrinks when the destination
    * degrades and grows back while it stays healthy.
    * </p>
    *
    * @return the maximum number of concurrent in-flight requests currently permitted to this destination.
    */
   public long concurrencyLimit() {
      return limitOf(this.packedLimit);
   }

   /**
    * Extracts the smoothed round-trip time from a packed state word.
    *
    * @param state the packed {@code packedState} value
    * @return SRTT in kibi-nanoseconds
    */
   private static long srttOf(long state) {
      return (state >>> SRTT_SHIFT) & LATENCY_MASK;
   }

   /**
    * Extracts the round-trip time variance from a packed state word.
    *
    * @param state the packed {@code packedState} value
    * @return RTTVAR in kibi-nanoseconds
    */
   private static long rttvarOf(long state) {
      return (state >>> RTTVAR_SHIFT) & LATENCY_MASK;
   }

   /**
    * Extracts the token bucket level from a packed state word.
    *
    * @param state the packed {@code packedState} value
    * @return bucket level, between 0 and {@link #MAX_BUCKET_LEVEL}
    */
   private static long bucketOf(long state) {
      return (state >>> BUCKET_SHIFT) & BUCKET_MASK;
   }

   /**
    * Extracts the probe counter from a packed state word.
    *
    * @param state the packed {@code packedState} value
    * @return probe counter, between 0 and {@link #PROBE_MASK}
    */
   private static long probeOf(long state) {
      return (state >>> PROBE_SHIFT) & PROBE_MASK;
   }

   /**
    * Extracts the adaptive concurrency limit from a packed limit word.
    *
    * @param limit the packed {@code packedLimit} value
    * @return the maximum number of concurrent in-flight requests currently permitted
    */
   private static long limitOf(long limit) {
      return (limit >>> LIMIT_SHIFT) & INT_MASK;
   }

   /**
    * Extracts the observed baseline round-trip time from a packed limit word.
    *
    * @param limit the packed {@code packedLimit} value
    * @return baseline RTT in kibi-nanoseconds, the lowest latency observed for this destination
    */
   private static long baseRttOf(long limit) {
      return (limit >>> BASE_RTT_SHIFT) & INT_MASK;
   }

   /**
    * Checks whether a packed state word has recorded at least one sample.
    *
    * @param state the packed {@code packedState} value
    * @return {@code true} if SRTT/RTTVAR hold a real estimate, {@code false} if still at their initial zero value
    */
   private static boolean isInitialized(long state) {
      return (state & INIT_BIT) != 0;
   }

   private static long pack(long srtt, long rttvar, long bucket, long probe, boolean init) {
      return srtt & LATENCY_MASK
            | (rttvar & LATENCY_MASK) << RTTVAR_SHIFT
            | (bucket & BUCKET_MASK) << BUCKET_SHIFT
            | (probe & PROBE_MASK) << PROBE_SHIFT
            | (init ? INIT_BIT : 0L);
   }
}
