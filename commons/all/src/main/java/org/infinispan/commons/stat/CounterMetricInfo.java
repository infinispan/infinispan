package org.infinispan.commons.stat;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Represents a counter. It sets {@link CounterTracker} into the target instance.
 * <p>
 * A counter tracks a monotonically increasing values and never resets to a lesser value.
 *
 * @since 15.0
 */
public final class CounterMetricInfo<T> extends BaseSetterMetricInfo<T, CounterTracker> {

   public CounterMetricInfo(String name, String description, Map<String, String> map, BiConsumer<T, CounterTracker> setter) {
      super(name, description, map, setter);
   }

}
