package org.infinispan.jmx;

/**
 * Interface containing common cache management operations
 *
 * @author Jerry Gauthier
 * @since 4.0
 */
public interface JmxStatisticsExposer {
   /**
    * Returns whether an interceptor's statistics are being captured.
    *
    * @return true if statistics are captured
    */
   boolean getStatisticsEnabled();

   /**
    * Enables an interceptor's cache statistics If true, the interceptor will capture statistics and make them available
    * through the mbean.
    *
    * @param enabled true if statistics should be captured
    */
   void setStatisticsEnabled(boolean enabled);

   /**
    * Resets an interceptor's cache statistics
    */
   void resetStatistics();
}
