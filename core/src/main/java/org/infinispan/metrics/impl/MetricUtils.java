package org.infinispan.metrics.impl;

import static org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.eclipse.microprofile.metrics.Timer;

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
    * @param name           The metric name.
    * @param description    The metric description.
    * @param getterFunction The {@link Function} invoked to return the metric value
    * @param <C>            The instance type.
    * @return The {@link AttributeMetadata} to be registered.
    */
   public static <C> AttributeMetadata createGauge(String name, String description,
         Function<C, Number> getterFunction) {
      return new AttributeMetadata(name, description, false, false, null, false, getterFunction, null);
   }

   /**
    * Creates a Timer metric.
    *
    * @param name           The metric name.
    * @param description    The metrics description.
    * @param setterFunction The {@link BiConsumer} invoked whit the {@link Timer} instance to update.
    * @param <C>            The instance type.
    * @return The {@link AttributeMetadata} to be registered.
    */
   public static <C> AttributeMetadata createTimer(String name, String description,
         BiConsumer<C, Timer> setterFunction) {
      return new AttributeMetadata(name, description, false, false, null, false, null, setterFunction);
   }

}
