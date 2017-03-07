package org.infinispan.stats;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
interface ClusterStats {
   /**
    * @return The time in milliseconds, to wait between requests before re-retrieving cluster wide stats
    */
   long getStaleStatsThreshold();

   /**
    * @param threshold the time in milliseconds, to wait between requests before re-retrieving cluster wide stats
    */
   void setStaleStatsThreshold(long threshold);

   /**
    * Reset the collected statistics
    */
   void reset();
}
