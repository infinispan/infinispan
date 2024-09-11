package org.infinispan.commons.stat.micrometer;

import org.infinispan.commons.stat.DistributionSummaryTracker;

import io.micrometer.core.instrument.DistributionSummary;

/**
 * A {@link DistributionSummaryTracker} implementation that stores the value in {@link DistributionSummary}.
 *
 * @since 15.1
 */
public record MicrometerDistributionSummary(DistributionSummary summary) implements DistributionSummaryTracker {

   @Override
   public void record(double amount) {
      summary.record(amount);
   }

}
