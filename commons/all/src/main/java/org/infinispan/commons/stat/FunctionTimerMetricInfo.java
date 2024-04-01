package org.infinispan.commons.stat;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Represents a duration metric. It sets {@link TimerTracker} into the target instance.
 * <p>
 * A distribution summary tracks a duration.
 *
 * @since 15.0
 */
public final class FunctionTimerMetricInfo<T> extends BaseSetterMetricInfo<T, TimerTracker> {

   public FunctionTimerMetricInfo(String name, String description, Map<String, String> map, BiConsumer<T, TimerTracker> setter) {
      super(name, description, map, setter);
   }


}
