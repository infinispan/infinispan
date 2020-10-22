package org.infinispan.query.core.stats;

/**
 * Exposes query and index statistics for a cache.
 *
 * @since 12.0
 */
public interface SearchStatistics {

   /**
    * @return {@link QueryStatistics}
    */
   QueryStatistics getQueryStatistics();

   /**
    * @return {@link IndexStatistics}
    */
   IndexStatistics getIndexStatistics();
}
