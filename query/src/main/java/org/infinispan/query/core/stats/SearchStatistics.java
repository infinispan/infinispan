package org.infinispan.query.core.stats;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.query.impl.ComponentRegistryUtils;

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


   /**
    * Obtains the statistics instance for the specified cache
    * @param cache the cache instance
    * @return the {@link SearchStatistics} instance of the specified cache
    */
   static <K, V> SearchStatistics of(Cache<K, V> cache) {
      return ComponentRegistryUtils.getSearchStatsRetriever(cache).getSearchStatistics();
   }

}
