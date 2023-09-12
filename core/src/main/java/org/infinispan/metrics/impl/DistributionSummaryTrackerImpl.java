package org.infinispan.metrics.impl;

import java.util.Objects;

import org.infinispan.commons.stat.DistributionSummaryTracker;

import io.micrometer.core.instrument.DistributionSummary;

/**
 * A {@link DistributionSummaryTracker} implementation that uses {@link DistributionSummary}.
 */
public class DistributionSummaryTrackerImpl implements DistributionSummaryTracker {

   private final DistributionSummary summary;

   DistributionSummaryTrackerImpl(DistributionSummary summary) {
      this.summary = Objects.requireNonNull(summary);
   }

   @Override
   public void record(double amount) {
      summary.record(amount);
   }
}
