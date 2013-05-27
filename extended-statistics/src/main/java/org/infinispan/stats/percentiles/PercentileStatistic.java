package org.infinispan.stats.percentiles;

/**
 * Percentile statistic for the transaction execution time.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public enum PercentileStatistic {
   RO_LOCAL_EXECUTION,
   WR_LOCAL_EXECUTION,
   RO_REMOTE_EXECUTION,
   WR_REMOTE_EXECUTION
}
