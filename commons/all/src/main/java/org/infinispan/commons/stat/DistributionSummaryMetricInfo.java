package org.infinispan.commons.stat;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Represents a distribution summary (histogram). It sets {@link DistributionSummaryTracker} into the target instance.
 * <p>
 * A distribution summary tracks the sample distribution of events.
 *
 * @since 15.0
 */
public final class DistributionSummaryMetricInfo<T> extends BaseSetterMetricInfo<T, DistributionSummaryTracker> {

   public DistributionSummaryMetricInfo(String name, String description, Map<String, String> map, BiConsumer<T, DistributionSummaryTracker> setter) {
      super(name, description, map, setter);
   }
}
