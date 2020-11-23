package org.infinispan.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The default {@link ExponentialBackOff} implementation for IRAC (asynchronous cross-site replication).
 * <p>
 * An exponential back-off implementation with min interval of 500 ms and a maximum of 300'000 ms (5 min). It uses a
 * multiplier of 2 (each timeslot will be increase + 100% for each consecutive retry) and the final wait time is
 * randomized, +- 50% of the timeslot.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class ExponentialBackOffImpl implements ExponentialBackOff {

   private static final Log log = LogFactory.getLog(ExponentialBackOffImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   //TODO currently only used by IRAC. If required, make it configurable (those 4 constants) to cover other uses cases.
   //multiplier value (2 == +100% per retry)
   private static final double MULTIPLIER = 2;
   //initial interval value in milliseconds
   private static final int INITIAL_INTERVAL_MILLIS = 500;
   //maximum back off time in milliseconds (300 seconds == 5 min)
   private static final int MAX_INTERVAL_MILLIS = 300_000;
   //randomization factor (0.5 == 50% below and 50% above the retry interval).
   private static final double RANDOMIZATION_FACTOR = 0.5;

   //the current retry timeout. If a retry occurs, it will wait for this time +- RANDOMIZATION_FACTOR (%)
   private int currentIntervalMillis;
   private final ScheduledExecutorService delayer;

   public ExponentialBackOffImpl(ScheduledExecutorService delayer) {
      this.delayer = delayer;
      this.currentIntervalMillis = INITIAL_INTERVAL_MILLIS;
   }

   long nextBackOffMillis() {
      //public for unit test purposes.
      if (currentIntervalMillis >= MAX_INTERVAL_MILLIS) {
         if (trace) {
            log.tracef("Next backoff time %s ms", MAX_INTERVAL_MILLIS);
         }
         return MAX_INTERVAL_MILLIS;
      }
      int randomIntervalMillis = getRandomValueFromInterval();
      incrementCurrentInterval();
      if (trace) {
         log.tracef("Next backoff time %s ms", randomIntervalMillis);
      }
      return Math.min(randomIntervalMillis, MAX_INTERVAL_MILLIS);
   }

   public void reset() {
      this.currentIntervalMillis = INITIAL_INTERVAL_MILLIS;
   }

   @Override
   public CompletionStage<Void> asyncBackOff() {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      long sleepTime = nextBackOffMillis();
      if (trace) {
         log.tracef("async backing-off for %s.", Util.prettyPrintTime(sleepTime));
      }
      delayer.schedule(() -> cf.complete(null), sleepTime, TimeUnit.MILLISECONDS);
      return cf;
   }

   private void incrementCurrentInterval() {
      // Check for overflow, if overflow is detected set the current interval to the max interval.
      if (currentIntervalMillis >= MAX_INTERVAL_MILLIS) {
         currentIntervalMillis = MAX_INTERVAL_MILLIS;
      } else {
         currentIntervalMillis *= MULTIPLIER;
      }
   }

   private int getRandomValueFromInterval() {
      double delta = RANDOMIZATION_FACTOR * currentIntervalMillis;
      return (int) (delta + (ThreadLocalRandom.current().nextDouble() * currentIntervalMillis));
   }

}
