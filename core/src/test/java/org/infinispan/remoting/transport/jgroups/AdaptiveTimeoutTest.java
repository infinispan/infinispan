package org.infinispan.remoting.transport.jgroups;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.assertj.core.data.Offset.offset;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class AdaptiveTimeoutTest {

   private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);

   @Test
   void srttConvergesOnStableLatency() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }

      assertThat(adaptive.srtt()).isCloseTo(10.0 * MS, withinPercentage(5));
      assertThat(adaptive.rttvar()).isCloseTo(0, offset(1.0 * MS));
   }

   @Test
   void adjustTimeoutBeforeAnySamplesReturnsConfigured() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      long configuredTimeout = 15_000 * MS;
      assertThat(adaptive.adjustTimeout(configuredTimeout)).isEqualTo(configuredTimeout);
   }

   @Test
   void fullBucketReturnsConfiguredTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }

      long configuredTimeout = 15_000 * MS;
      assertThat(adaptive.adjustTimeout(configuredTimeout)).isEqualTo(configuredTimeout);
   }

   @Test
   void consecutiveTimeoutsDrainBucket() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }

      for (int i = 0; i < 5; i++) {
         adaptive.recordTimeout();
      }

      assertThat(adaptive.bucketLevel()).isEqualTo(0);
   }

   @Test
   void emptyBucketReturnsFloorTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }
      for (int i = 0; i < 5; i++) {
         adaptive.recordTimeout();
      }

      long configuredTimeout = 15_000 * MS;
      long minEffective = Long.MAX_VALUE;
      for (int i = 0; i < 3; i++) {
         minEffective = Math.min(minEffective, adaptive.adjustTimeout(configuredTimeout));
      }

      // Stable 10ms latency, variance ≈ 0, so floor ≈ srtt ≈ 10ms
      assertThat(minEffective).isCloseTo(10 * MS, offset(2 * MS));
   }

   @Test
   void partialBucketInterpolatesBetweenFloorAndConfigured() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }

      adaptive.recordTimeout();
      adaptive.recordTimeout();

      long configuredTimeout = 15_000 * MS;
      long effective = adaptive.adjustTimeout(configuredTimeout);

      assertThat(effective).isGreaterThan(10 * MS);
      assertThat(effective).isLessThan(configuredTimeout);
   }

   @Test
   void varianceIncreasesAdaptiveFloor() {
      AdaptiveTimeout stable = new AdaptiveTimeout();
      for (int i = 0; i < 20; i++) {
         stable.recordSuccess(10 * MS);
      }

      AdaptiveTimeout jittery = new AdaptiveTimeout();
      for (int i = 0; i < 20; i++) {
         jittery.recordSuccess((i % 2 == 0 ? 5 : 15) * MS);
      }

      for (int i = 0; i < 5; i++) {
         stable.recordTimeout();
         jittery.recordTimeout();
      }

      long configuredTimeout = 15_000 * MS;

      long stableFloor = Long.MAX_VALUE;
      long jitteryFloor = Long.MAX_VALUE;
      for (int i = 0; i < 3; i++) {
         stableFloor = Math.min(stableFloor, stable.adjustTimeout(configuredTimeout));
         jitteryFloor = Math.min(jitteryFloor, jittery.adjustTimeout(configuredTimeout));
      }

      assertThat(jitteryFloor).isGreaterThan(stableFloor);
   }

   @Test
   void floorNeverExceedsConfiguredTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(20_000 * MS);
      }
      for (int i = 0; i < 5; i++) {
         adaptive.recordTimeout();
      }

      long configuredTimeout = 15_000 * MS;
      long effective = adaptive.adjustTimeout(configuredTimeout);

      assertThat(effective).isLessThanOrEqualTo(configuredTimeout);
   }

   @Test
   void inFlightCircuitBreakerNeverExceedsConfiguredTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      // A destination that eventually responds, but slower than the configured timeout itself
      // (e.g. a node buried under backlog that still drains, just past the deadline callers care about).
      // This poisons the SRTT-derived floor above the configured timeout.
      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(20_000 * MS);
      }

      long configuredTimeout = 15_000 * MS;

      // A backlog piles up on top of the already-poisoned SRTT, tripping the Little's-Law
      // circuit breaker in adjustTimeout().
      for (int i = 0; i < 50; i++) {
         adaptive.incrementInFlight();
      }

      long effective = adaptive.adjustTimeout(configuredTimeout);

      // A floor this far above configured (~20.17s vs 15s) should shrink the effective timeout well below
      // configured, not just clamp back to it — the worse the floor overshoot, the more confident we are the
      // destination is broken, so the timeout should shrink further, not revert to "no adaptation."
      assertThat(effective)
            .as("adaptive timeout must shrink below the configured timeout, reflecting how badly the floor overshot it")
            .isCloseTo(11_156 * MS, offset(200 * MS));
   }

   @Test
   void adjustTimeoutWithSubMicroTimeoutDoesNotThrow() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      // Sub-microsecond latency (e.g. loopback/same-host) rounds down to a zero floor. Paired with a
      // zero configured timeout, this must not divide by zero in the bounded-floor calculation.
      adaptive.recordSuccess(0);

      assertThat(adaptive.adjustTimeout(0)).isEqualTo(0);
      assertThat(adaptive.adjustTimeout(1)).isEqualTo(1);
   }

   @Test
   void gradualRecoveryAfterDrain() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }
      for (int i = 0; i < 5; i++) {
         adaptive.recordTimeout();
      }

      long configuredTimeout = 15_000 * MS;

      // Capture the floor (min of several calls to skip any probes)
      long floorTimeout = Long.MAX_VALUE;
      for (int i = 0; i < 3; i++) {
         floorTimeout = Math.min(floorTimeout, adaptive.adjustTimeout(configuredTimeout));
      }

      for (int i = 0; i < 3; i++) {
         adaptive.recordSuccess(10 * MS);
      }

      long partialRecovery = adaptive.adjustTimeout(configuredTimeout);

      assertThat(partialRecovery).isGreaterThan(floorTimeout);
      assertThat(partialRecovery).isLessThan(configuredTimeout);
   }

   @Test
   void highInFlightReducesTimeoutBeforeAnyTimeoutFires() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      // Train SRTT to 10ms with healthy traffic. Each request starts and completes,
      // so in-flight returns to zero after training.
      for (int i = 0; i < 20; i++) {
         adaptive.incrementInFlight();
         adaptive.recordSuccess(10 * MS);
         adaptive.removeInFlight();
      }

      // Bucket is full, node looks healthy.
      assertThat(adaptive.adjustTimeout(15_000 * MS))
            .as("without congestion, full bucket returns configured timeout")
            .isEqualTo(15_000 * MS);

      // Simulate pileup: 200 requests start, none complete.
      // With SRTT ~10ms, estimated queue depth is 200 * 10ms = 2s.
      for (int i = 0; i < 200; i++) {
         adaptive.incrementInFlight();
      }

      long configuredTimeout = 15_000 * MS;
      long adjusted = adaptive.adjustTimeout(configuredTimeout);

      assertThat(adjusted)
            .as("high in-flight should reduce timeout even with full bucket")
            .isLessThan(configuredTimeout);
      assertThat(adjusted)
            .as("timeout must remain positive")
            .isGreaterThan(0);
   }

   @Test
   void inFlightReductionScalesWithCongestion() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.incrementInFlight();
         adaptive.recordSuccess(10 * MS);
         adaptive.removeInFlight();
      }

      long configuredTimeout = 15_000 * MS;

      // Moderate pileup
      for (int i = 0; i < 50; i++) {
         adaptive.incrementInFlight();
      }
      long moderateAdjusted = adaptive.adjustTimeout(configuredTimeout);

      // Severe pileup (add more on top)
      for (int i = 0; i < 500; i++) {
         adaptive.incrementInFlight();
      }
      long severeAdjusted = adaptive.adjustTimeout(configuredTimeout);

      assertThat(severeAdjusted)
            .as("more congestion should produce shorter timeout")
            .isLessThan(moderateAdjusted);
   }

   @Test
   void inFlightDecrementsOnCompletionRestoresTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.incrementInFlight();
         adaptive.recordSuccess(10 * MS);
         adaptive.removeInFlight();
      }

      long configuredTimeout = 15_000 * MS;

      // Build up in-flight: 200 requests start but none complete yet.
      for (int i = 0; i < 200; i++) {
         adaptive.incrementInFlight();
      }

      long congestedTimeout = adaptive.adjustTimeout(configuredTimeout);
      assertThat(congestedTimeout).isLessThan(configuredTimeout);

      // Drain all in-flight via completions. A completion records latency and releases the slot.
      for (int i = 0; i < 200; i++) {
         adaptive.recordSuccess(10 * MS);
         adaptive.removeInFlight();
      }

      long restoredTimeout = adaptive.adjustTimeout(configuredTimeout);
      assertThat(restoredTimeout)
            .as("after in-flight drains, timeout should recover to configured")
            .isEqualTo(configuredTimeout);
   }

   @Test
   void shouldShedReturnsFalseWellBelowThreshold() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      // A handful of in-flight requests is normal, healthy load - nowhere near the shedding threshold.
      for (int i = 0; i < 50; i++) {
         adaptive.incrementInFlight();
      }

      assertThat(adaptive.shouldShed())
            .as("moderate in-flight load should not trigger shedding")
            .isFalse();
   }

   @Test
   void shouldShedWhenBacklogExceedsDegradedLimit() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      // Consecutive timeouts shrink the adaptive concurrency limit for this destination.
      for (int i = 0; i < 4; i++) {
         adaptive.recordTimeout();
      }

      // The in-flight backlog now exceeds the shrunken limit, so the destination sheds. A healthy destination
      // (full limit) would tolerate the same backlog, as shouldShedReturnsFalseWellBelowThreshold covers.
      for (int i = 0; i < 20_000; i++) {
         adaptive.incrementInFlight();
      }

      assertThat(adaptive.shouldShed())
            .as("a degraded destination sheds once its backlog exceeds the shrunken concurrency limit")
            .isTrue();
   }

   @Test
   void probeReturnsGenerousTimeoutNearEmptyBucket() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }
      for (int i = 0; i < 5; i++) {
         adaptive.recordTimeout();
      }

      long configuredTimeout = 15_000 * MS;

      boolean sawProbe = false;
      boolean sawFloor = false;
      for (int i = 0; i < 20; i++) {
         long effective = adaptive.adjustTimeout(configuredTimeout);
         if (effective == configuredTimeout) {
            sawProbe = true;
         } else {
            sawFloor = true;
         }
      }

      assertThat(sawProbe).as("expected at least one probe with generous timeout").isTrue();
      assertThat(sawFloor).as("expected most calls to return floor timeout").isTrue();
   }

   @Test
   void shouldHaveHealthyTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 200; i++) {
         adaptive.recordSuccess(1);
      }

      // The latency is so fast that srtt moves to 0.
      assertThat(adaptive.srtt()).isEqualTo(0.0);

      // After several healthy records, some timeouts start to happen.
      adaptive.recordTimeout();
      adaptive.recordTimeout();
      adaptive.recordTimeout();
      adaptive.recordTimeout();

      long baseTimeout = 15_000 * MS;

      // The timeout should never be zero.
      assertThat(adaptive.adjustTimeout(baseTimeout))
            .isGreaterThan(0)
            .isLessThan(baseTimeout);
   }

   @Test
   void testLimitWithSpuriousTimeouts() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();

      for (int i = 0; i < 200; i++) {
         adaptive.recordTimeout();
         adaptive.recordSuccess(1);
         adaptive.recordSuccess(1);
         adaptive.recordSuccess(1);
         adaptive.recordTimeout();

         assertThat(adaptive.concurrencyLimit()).isGreaterThanOrEqualTo(128);
      }

      // Limit will gradually recover with successes.
      long limit = adaptive.concurrencyLimit();
      for (int i = 0; i < 200; i++) {
         adaptive.recordSuccess(1);
         assertThat(adaptive.concurrencyLimit()).isGreaterThan(limit);
         limit = adaptive.concurrencyLimit();
      }
   }
}
