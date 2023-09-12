package org.infinispan.commons.stat;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Represents a duration event (histogram). It sets {@link TimerTracker} into the target instance.
 *
 * @since 15.0
 */
public final class TimerMetricInfo<T> extends BaseSetterMetricInfo<T, TimerTracker> {

   public TimerMetricInfo(String name, String description, Map<String, String> map, BiConsumer<T, TimerTracker> setter) {
      super(name, description, map, setter);
   }
}
