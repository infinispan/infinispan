package org.infinispan.commons.stat;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Same as {@link GaugeMetricInfo} but the tracked value is a time or duration.
 *
 * @since 15.0
 */
public final class TimeGaugeMetricInfo<T> extends BaseMetricInfo {

   private final Function<T, Number> function;
   private final TimeUnit timeUnit;

   public TimeGaugeMetricInfo(String name, String description, Map<String, String> map, Function<T, Number> function, TimeUnit timeUnit) {
      super(name, description, map);
      this.function = Objects.requireNonNull(function);
      this.timeUnit = Objects.requireNonNull(timeUnit);
   }

   /**
    * @return The {@link Supplier} to invoke to return the current value of the metric.
    */
   public Supplier<Number> getGauge(T instance) {
      return () -> function.apply(instance);
   }

   /**
    * @return The {@link TimeUnit} of the tracked value.
    */
   public TimeUnit getTimeUnit() {
      return timeUnit;
   }
}
