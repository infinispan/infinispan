package org.infinispan.commons.stat;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents a gauge metric.
 * <p>
 * A gauge tracks a specific value, like a queue size.
 *
 * @since 15.0
 */
public final class GaugeMetricInfo<T> extends BaseMetricInfo {

   private final Function<T, Number> function;

   public GaugeMetricInfo(String name, String description, Map<String, String> map, Function<T, Number> function) {
      super(name, description, map);
      this.function = Objects.requireNonNull(function);
   }

   /**
    * @return The {@link Supplier} to be invoked to fetch the value from {@code instance}.
    */
   public Supplier<Number> getGauge(T instance) {
      return () -> function.apply(instance);
   }
}
