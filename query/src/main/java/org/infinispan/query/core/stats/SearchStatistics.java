package org.infinispan.query.core.stats;

import java.util.concurrent.CompletionStage;

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

   /**
    * @return A snapshot of self.
    */
   CompletionStage<SearchStatisticsSnapshot> computeSnapshot();

}
