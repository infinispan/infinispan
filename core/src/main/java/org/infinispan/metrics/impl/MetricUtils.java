package org.infinispan.metrics.impl;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commons.stat.CounterMetricInfo;
import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryMetricInfo;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.FunctionTimerMetricInfo;
import org.infinispan.commons.stat.GaugeMetricInfo;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.stat.TimerMetricInfo;
import org.infinispan.commons.stat.TimerTracker;

/**
 * Utility methods for metrics.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public final class MetricUtils {

   private MetricUtils() {
   }


   /**
    * Creates a Gauge metric.
    *
    * @param <C>         The instance type to track.
    * @param name        The metric name.
    * @param description The metric description.
    * @param function    The {@link Function} invoked to return the metric value.
    * @param tags        Additional metric tags.
    * @return The {@link MetricInfo} to be registered.
    */
   public static <C> GaugeMetricInfo<C> createGauge(String name, String description,
                                                    Function<C, Number> function,
                                                    Map<String, String> tags) {
      return new GaugeMetricInfo<>(name, description, tags, function);
   }

   /**
    * Creates a Timer metric.
    *
    * @param <C>         The instance type to track.
    * @param name        The metric name.
    * @param description The metrics description.
    * @param consumer    The {@link BiConsumer} invoked with the {@link TimerTracker} instance to update.
    * @param tags        Additional metric tags.
    * @return The {@link MetricInfo} to be registered.
    */
   public static <C> TimerMetricInfo<C> createTimer(String name, String description,
                                                    BiConsumer<C, TimerTracker> consumer,
                                                    Map<String, String> tags) {
      return new TimerMetricInfo<>(name, description, tags, consumer);
   }

   /**
    * Creates a Counter metric.
    *
    * @param name        The metric name.
    * @param description The metrics description.
    * @param consumer    The {@link BiConsumer} invoked with the {@link CounterTracker} instance to update.
    * @param tags        Additional metric tags.
    * @param <C>         The instance type to track.
    * @return The {@link CounterMetricInfo} to be registered.
    */
   public static <C> CounterMetricInfo<C> createCounter(String name, String description,
                                                        BiConsumer<C, CounterTracker> consumer,
                                                        Map<String, String> tags) {
      return new CounterMetricInfo<>(name, description, tags, consumer);
   }

   /**
    * Creates a Timer metric without histogram support.
    * <p>
    * The records duration will be stored in {@link TimeUnit#NANOSECONDS}
    *
    * @param name        The metric name.
    * @param description The metrics description.
    * @param consumer    The {@link BiConsumer} invoked with the {@link TimerTracker} instance to update.
    * @param tags        Additional metric tags.
    * @param <C>         The instance type to track.
    * @return The {@link FunctionTimerMetricInfo} to be registered.
    * @see #createFunctionTimer(String, String, BiConsumer, Map)
    */
   public static <C> FunctionTimerMetricInfo<C> createFunctionTimer(String name, String description,
                                                                    BiConsumer<C, TimerTracker> consumer,
                                                                    Map<String, String> tags) {
      return new FunctionTimerMetricInfo<>(name, description, tags, consumer);
   }

   /**
    * Creates a Distribution Summary metric.
    *
    * @param name        The metric name.
    * @param description The metrics description.
    * @param consumer    The {@link BiConsumer} invoked with the {@link DistributionSummaryTracker} instance to update.
    * @param tags        Additional metric tags.
    * @param <C>         The instance type to track.
    * @return The {@link DistributionSummaryMetricInfo} to be registered.
    */
   public static <C> DistributionSummaryMetricInfo<C> createDistributionSummary(String name, String description,
                                                                                BiConsumer<C, DistributionSummaryTracker> consumer,
                                                                                Map<String, String> tags) {
      return new DistributionSummaryMetricInfo<>(name, description, tags, consumer);
   }

}
