package org.infinispan.commons.stat;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Extends {@link BaseMetricInfo} with a consumer, usually a target instance and the tracker to set.
 *
 * @since 15.0
 */
abstract class BaseSetterMetricInfo<O, M> extends BaseMetricInfo {

   private final BiConsumer<O, M> setter;

   BaseSetterMetricInfo(String name, String description, Map<String, String> map, BiConsumer<O, M> setter) {
      super(name, description, map);
      this.setter = Objects.requireNonNull(setter);
   }

   public final void accept(O targetInstance, M metricTracker) {
      setter.accept(targetInstance, metricTracker);
   }
}
