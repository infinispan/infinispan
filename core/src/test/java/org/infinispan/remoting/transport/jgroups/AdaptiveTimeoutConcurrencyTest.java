package org.infinispan.remoting.transport.jgroups;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.junit.jupiter.api.Test;

/**
 * Concurrency coverage for {@link AdaptiveTimeout}.
 *
 * <p>
 * The estimator, the concurrency limiter and the in-flight counter each live in their own word and are mutated with a
 * lock-free compare-and-exchange loop. Single-threaded tests cannot exercise the retry paths, so these tests drive every
 * word from many threads at once and assert the guarantees that only concurrency can break: the counter stays exact, the
 * packed fields stay within range, the estimate still converges, and no reader ever observes an out-of-range timeout.
 * </p>
 */
public class AdaptiveTimeoutConcurrencyTest {

   private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);
   private static final long CONFIGURED_TIMEOUT = 15_000 * MS;
   private static final int THREADS = Math.max(4, ProcessorInfo.availableProcessors());
   private static final long JOIN_TIMEOUT_SECONDS = 60;

   @Test
   void inFlightCounterIsAtomicUnderContention() throws Exception {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();
      int incrementsPerThread = 200_000;

      runConcurrently(() -> {
         for (int i = 0; i < incrementsPerThread; i++) {
            adaptive.incrementInFlight();
         }
      });

      assertThat(adaptive.inFlight())
            .as("every concurrent increment must be observed, none lost to a non-atomic update")
            .isEqualTo(THREADS * incrementsPerThread);

      runConcurrently(() -> {
         for (int i = 0; i < incrementsPerThread; i++) {
            adaptive.removeInFlight();
         }
      });

      assertThat(adaptive.inFlight())
            .as("balanced increments and decrements must net back to zero")
            .isZero();
   }

   @Test
   void srttConvergesUnderConcurrentSuccesses() throws Exception {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();
      long latency = 10 * MS;
      int successesPerThread = 100_000;

      runConcurrently(() -> {
         for (int i = 0; i < successesPerThread; i++) {
            adaptive.recordSuccess(latency);
         }
      });

      // The exponential moving average of a constant sample has that constant as its fixed point, so regardless of how
      // the concurrent updates interleave the estimate must settle on the recorded latency.
      assertThat(adaptive.srtt())
            .as("concurrent successes on a stable latency must still converge the estimate")
            .isCloseTo((double) latency, withinPercentage(10));

      assertThat(adaptive.bucketLevel())
            .as("a flood of successes must leave the bucket saturated, never above capacity")
            .isEqualTo(4.0);

      assertFieldsInRange(adaptive);
   }

   @Test
   void invariantsHoldUnderConcurrentMixedLoad() throws Exception {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();
      int iterationsPerThread = 200_000;
      AtomicReference<String> violation = new AtomicReference<>();

      runConcurrently(() -> {
         ThreadLocalRandom random = ThreadLocalRandom.current();
         for (int i = 0; i < iterationsPerThread; i++) {
            // Every iteration holds an in-flight slot around its operation, so the counter oscillates and the
            // Little's-law branch of adjustTimeout runs against genuinely concurrent in-flight readings.
            adaptive.incrementInFlight();
            try {
               int choice = random.nextInt(10);
               if (choice < 4) {
                  adaptive.recordSuccess(random.nextLong(1, 50 * MS));
               } else if (choice < 6) {
                  adaptive.recordTimeout();
               } else if (choice < 9) {
                  long effective = adaptive.adjustTimeout(CONFIGURED_TIMEOUT);
                  if (effective < 0 || effective > CONFIGURED_TIMEOUT) {
                     violation.compareAndSet(null, "adjustTimeout returned " + effective);
                  }
               } else {
                  adaptive.shouldShed();
               }
            } finally {
               adaptive.removeInFlight();
            }
         }
      });

      assertThat(violation.get())
            .as("no reader may observe a timeout outside the configured bound under concurrency")
            .isNull();
      assertThat(adaptive.inFlight())
            .as("balanced in-flight bookkeeping must net back to zero")
            .isZero();
      assertFieldsInRange(adaptive);
   }

   private static void assertFieldsInRange(AdaptiveTimeout adaptive) {
      assertThat(adaptive.bucketLevel())
            .as("bucket level stays within its capacity")
            .isBetween(0.0, 4.0);
      assertThat(adaptive.srtt())
            .as("smoothed round-trip time never decodes to a negative or corrupt value")
            .isGreaterThanOrEqualTo(0.0)
            .isLessThan((double) (500 * MS));
      assertThat(adaptive.rttvar())
            .as("round-trip variance never decodes to a negative or corrupt value")
            .isGreaterThanOrEqualTo(0.0)
            .isLessThan((double) (500 * MS));
   }

   private static void runConcurrently(Runnable runnable) throws Exception {
      ExecutorService executor = Executors.newFixedThreadPool(THREADS);
      AtomicReference<Throwable> error = new AtomicReference<>();
      CyclicBarrier barrier = new CyclicBarrier(THREADS);
      try {
         AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
         for (int t = 0; t < THREADS; t++) {
            acs.dependsOn(CompletableFuture.runAsync(() -> {
               try {
                  awaitBarrier(barrier);
                  runnable.run();
               } catch (Throwable throwable) {
                  error.compareAndSet(null, throwable);
               }
               }, executor));
         }

         acs.freeze().toCompletableFuture().get(JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } finally {
         executor.shutdownNow();
         assertThat(executor.awaitTermination(JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
               .as("worker pool must terminate")
               .isTrue();
      }

      assertThat(error.get())
            .as("no worker may fail under concurrent load")
            .isNull();
   }

   private static void awaitBarrier(CyclicBarrier barrier) {
      try {
         barrier.await(JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (Exception e) {
         throw new IllegalStateException("failed to synchronise workers at the start barrier", e);
      }
   }
}
