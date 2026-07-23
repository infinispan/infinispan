package org.infinispan.remoting.transport.jgroups;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.stat.GaugeMetricInfo;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.metrics.Constants;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link JGroupsMetricsManagerImpl} exposes the per-destination {@link AdaptiveTimeout} accessors as
 * gauges.
 *
 * <p>
 * These tests assert wiring only: that each gauge reads the correct accessor of the live {@link AdaptiveTimeout}
 * instance and carries the {@code target_node} tag. The estimator's own arithmetic is covered by
 * {@code AdaptiveTimeoutTest}, so gauge values are compared against the accessor itself rather than against a computed
 * expectation.
 * </p>
 */
public class JGroupsMetricsManagerGaugesTest {

   @Test
   void roundTripTimeGaugeReadsSmoothedRtt() {
      String dst = "node-a";
      AdaptiveTimeout adaptiveTimeout = new AdaptiveTimeout();
      // Put the estimator in a non-trivial state so srtt() differs from the other accessors; this guards against a gauge
      // accidentally bound to the wrong accessor. We do not assert what the value is, only that the gauge mirrors it.
      adaptiveTimeout.recordSuccess(TimeUnit.MILLISECONDS.toNanos(10));

      GaugeMetricInfo<AdaptiveTimeout> gauge =
            findGauge(JGroupsMetricsManagerImpl.adaptiveTimeoutGauges(dst), "RoundTripTime");

      assertThat(gauge.getGauge(adaptiveTimeout).get().doubleValue())
            .as("RoundTripTime gauge must report the destination's smoothed RTT")
            .isEqualTo(adaptiveTimeout.srtt());

      assertThat(gauge.getTags())
            .as("gauge must be tagged with the target node")
            .containsEntry(Constants.TARGET_NODE, dst);
   }

   @Test
   void roundTripTimeVarianceGaugeReadsRttVariance() {
      String dst = "node-a";
      AdaptiveTimeout adaptiveTimeout = new AdaptiveTimeout();
      // After a sample, rttvar() is half of srtt(), so a gauge wrongly bound to srtt() would report a different value.
      adaptiveTimeout.recordSuccess(TimeUnit.MILLISECONDS.toNanos(10));

      GaugeMetricInfo<AdaptiveTimeout> gauge =
            findGauge(JGroupsMetricsManagerImpl.adaptiveTimeoutGauges(dst), "RoundTripTimeVariance");

      assertThat(gauge.getGauge(adaptiveTimeout).get().doubleValue())
            .as("RoundTripTimeVariance gauge must report the destination's RTT variance")
            .isEqualTo(adaptiveTimeout.rttvar());

      assertThat(gauge.getTags())
            .as("gauge must be tagged with the target node")
            .containsEntry(Constants.TARGET_NODE, dst);
   }

   @Test
   void tokenBucketLevelGaugeReadsBucketLevel() {
      String dst = "node-a";
      AdaptiveTimeout adaptiveTimeout = new AdaptiveTimeout();
      // Drain the bucket so its level is a distinct, non-default value, separate from the latency accessors.
      adaptiveTimeout.recordSuccess(TimeUnit.MILLISECONDS.toNanos(10));
      adaptiveTimeout.recordTimeout();

      GaugeMetricInfo<AdaptiveTimeout> gauge =
            findGauge(JGroupsMetricsManagerImpl.adaptiveTimeoutGauges(dst), "TokenBucketLevel");

      assertThat(gauge.getGauge(adaptiveTimeout).get().doubleValue())
            .as("TokenBucketLevel gauge must report the destination's token bucket level")
            .isEqualTo(adaptiveTimeout.bucketLevel());

      assertThat(gauge.getTags())
            .as("gauge must be tagged with the target node")
            .containsEntry(Constants.TARGET_NODE, dst);
   }

   @Test
   void inFlightRequestsGaugeReadsInFlightCount() {
      String dst = "node-a";
      AdaptiveTimeout adaptiveTimeout = new AdaptiveTimeout();
      // Raise the in-flight counter to a distinct, non-zero value.
      adaptiveTimeout.incrementInFlight();
      adaptiveTimeout.incrementInFlight();
      adaptiveTimeout.incrementInFlight();

      GaugeMetricInfo<AdaptiveTimeout> gauge =
            findGauge(JGroupsMetricsManagerImpl.adaptiveTimeoutGauges(dst), "InFlightRequests");

      assertThat(gauge.getGauge(adaptiveTimeout).get().intValue())
            .as("InFlightRequests gauge must report the destination's in-flight request count")
            .isEqualTo(adaptiveTimeout.inFlight());

      assertThat(gauge.getTags())
            .as("gauge must be tagged with the target node")
            .containsEntry(Constants.TARGET_NODE, dst);
   }

   @Test
   void concurrencyLimitGaugeReadsConcurrencyLimit() {
      String dst = "node-a";
      AdaptiveTimeout adaptiveTimeout = new AdaptiveTimeout();

      GaugeMetricInfo<AdaptiveTimeout> gauge =
            findGauge(JGroupsMetricsManagerImpl.adaptiveTimeoutGauges(dst), "ConcurrencyLimit");

      assertThat(gauge.getGauge(adaptiveTimeout).get().longValue())
            .as("ConcurrencyLimit gauge must report the destination's adaptive concurrency limit")
            .isEqualTo(adaptiveTimeout.concurrencyLimit());

      assertThat(gauge.getTags())
            .as("gauge must be tagged with the target node")
            .containsEntry(Constants.TARGET_NODE, dst);
   }

   @SuppressWarnings("unchecked")
   private static GaugeMetricInfo<AdaptiveTimeout> findGauge(Collection<MetricInfo> attributes, String name) {
      return (GaugeMetricInfo<AdaptiveTimeout>) attributes.stream()
            .filter(attribute -> attribute.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no gauge named " + name));
   }
}
