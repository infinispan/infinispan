package org.infinispan.commons.stat;

/**
 * Track the sample distribution of events.
 */
public interface DistributionSummaryTracker {

   DistributionSummaryTracker NO_OP = amount -> {
      //no-op
   };


   /**
    * Updates the statistics kept by the summary with the specified amount.
    *
    * @param amount Amount for an event being tracked.
    */
   void record(double amount);

}
