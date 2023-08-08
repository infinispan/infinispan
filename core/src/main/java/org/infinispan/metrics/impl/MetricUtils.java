package org.infinispan.metrics.impl;

import static org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

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
    * @param <C>            The instance type.
    * @param name           The metric name.
    * @param description    The metric description.
    * @param getterFunction The {@link Function} invoked to return the metric value.
    * @param tags           The metric tags if supported.
    * @return The {@link AttributeMetadata} to be registered.
    */
   public static <C> AttributeMetadata createGauge(String name, String description,
                                                   Function<C, Number> getterFunction,
                                                   Map<String, String> tags) {
      return new AttributeMetadata(name, description, false, false, null, false, getterFunction, null, false, tags);
   }

   /**
    * Creates a Timer metric.
    *
    * @param <C>            The instance type.
    * @param name           The metric name.
    * @param description    The metrics description.
    * @param setterFunction The {@link BiConsumer} invoked with the {@link TimerTracker} instance to update.
    * @param tags           The metric tags if supported.
    * @return The {@link AttributeMetadata} to be registered.
    */
   public static <C> AttributeMetadata createTimer(String name, String description,
                                                   BiConsumer<C, TimerTracker> setterFunction,
                                                   Map<String, String> tags) {
      return new AttributeMetadata(name, description, false, false, null, false, null, setterFunction, false, tags);
   }

}
