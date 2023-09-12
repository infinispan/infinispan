package org.infinispan.commons.stat;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Represents a duration metric. It sets {@link TimerTracker} into the target instance.
 * <p>
 * A distribution summary tracks a duration.
 *
 * @since 15.0
 */
public final class FunctionTimerMetricInfo<T> extends BaseSetterMetricInfo<T, TimerTracker> {

   private final TimeUnit timeUnit;

   public FunctionTimerMetricInfo(String name, String description, Map<String, String> map, BiConsumer<T, TimerTracker> setter, TimeUnit timeUnit) {
      super(name, description, map, setter);
      this.timeUnit = Objects.requireNonNull(timeUnit);
   }

   /**
    * @return The {@link TimeUnit} of the time tracked by the event.
    */
   public TimeUnit getTimeUnit() {
      return timeUnit;
   }

   @Override
   public String toString() {
      return "FunctionTimerMetricInfo{" +
            "name='" + getName() + '\'' +
            ", timeUnit=" + timeUnit +
            ", description='" + getDescription() + '\'' +
            ", tags=" + getTags() +
            '}';
   }

}
