package org.infinispan.client.hotrod.metrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.TimerTracker;

/**
 * A metrics registry.
 * <p>
 * Allows creating different types of metrics and keeps track of the ones registered. The method {@link #close()} will
 * remove and unregister all metrics created by this instance.
 *
 * @since 15.1
 */
public interface HotRodClientMetricsRegistry {

   HotRodClientMetricsRegistry DISABLED = new HotRodClientMetricsRegistry() {
      @Override
      public void createGauge(String metricName, String description, Supplier<Number> gauge, Map<String, String> tags, Consumer<Object> generatedId) {
         //no-op
      }

      @Override
      public void createTimeGauge(String metricName, String description, Supplier<Number> gauge, TimeUnit timeUnit, Map<String, String> tags, Consumer<Object> generatedId) {
         //no-op
      }

      @Override
      public TimerTracker createTimer(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
         return TimerTracker.NO_OP;
      }

      @Override
      public CounterTracker createCounter(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
         return CounterTracker.NO_OP;
      }

      @Override
      public DistributionSummaryTracker createDistributionSummery(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
         return DistributionSummaryTracker.NO_OP;
      }

      @Override
      public void close() {
         //no-op
      }

      @Override
      public void removeMetric(Object id) {
         //no-op
      }
   };

   /**
    * Creates a gauge metric.
    * <p>
    * A gauge keeps track of a single value. For example, a connection pool size.
    *
    * @param metricName  The metric name.
    * @param description A small description of the metrics to help explain what it measures.
    * @param gauge       The {@link Supplier} to be invoked to get the current value.
    * @param tags        Extra tags or information about the gauge (for example, cache name).
    * @param generatedId A {@link Consumer} to accept the generated ID. The ID will be used to unregister the metric on
    *                    demand.
    */
   void createGauge(String metricName, String description, Supplier<Number> gauge, Map<String, String> tags, Consumer<Object> generatedId);

   /**
    * Creates a time gauge metric.
    * <p>
    * A specialized gauge keeps track of a time value.
    *
    * @param metricName  The metric name.
    * @param description A small description of the metrics to help explain what it measures.
    * @param gauge       The {@link Supplier} to be invoked to get the current value.
    * @param timeUnit    The {@link TimeUnit} of the return value.
    * @param tags        Extra tags or information about the gauge (for example, cache name).
    * @param generatedId A {@link Consumer} to accept the generated ID. The ID will be used to unregister the metric on
    *                    demand.
    */
   void createTimeGauge(String metricName, String description, Supplier<Number> gauge, TimeUnit timeUnit, Map<String, String> tags, Consumer<Object> generatedId);

   /**
    * Creates a timer metrics.
    * <p>
    * A time keep track of the event's duration. For example, a request duration.
    * <p>
    * It can be used to compute more complex statistics, like histograms and/or percentiles.
    * <p>
    * The Hot Rod client uses the returned {@link TimerTracker} to register the event duration.
    *
    * @param metricName  The metric name.
    * @param description A small description of the metrics to help explain what it measures.
    * @param tags        Extra tags or information about the gauge (for example, cache name).
    * @return A {@link TimerTracker} implementation.
    */
   TimerTracker createTimer(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId);

   /**
    * Create a counter metric.
    * <p>
    * It keeps track of counting events and can never be decreased or reset. For example, the number of cache
    * hits/misses.
    * <p>
    * The Hot Rod client uses the returned {@link CounterTracker} to update the counter.
    *
    * @param metricName  The metric name.
    * @param description A small description of the metrics to help explain what it measures.
    * @param tags        Extra tags or information about the gauge (for example, cache name).
    * @param generatedId A {@link Consumer} to accept the generated ID. The ID will be used to unregister the metric on
    *                    demand.
    * @return The {@link CounterTracker} implementation.
    */
   CounterTracker createCounter(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId);

   /**
    * Creates a sample distribution metric.
    * <p>
    * It keeps track of the event's sample distribution.
    * <p>
    * It can be used to create more complex statistics like histograms.
    * <p>
    * The Hot Rod client uses the returned {@link DistributionSummaryTracker} to sample the events.
    *
    * @param metricName  The metric name.
    * @param description A small description of the metrics to help explain what it measures.
    * @param tags        Extra tags or information about the gauge (for example, cache name).
    * @param generatedId A {@link Consumer} to accept the generated ID. The ID will be used to unregister the metric on
    *                    demand.
    * @return The {@link DistributionSummaryTracker} implementation.
    */
   DistributionSummaryTracker createDistributionSummery(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId);

   /**
    * Unregister all metrics created.
    */
   void close();

   /**
    * Unregister a metric by its ID.
    *
    * @param id The metric ID.
    */
   void removeMetric(Object id);

}
