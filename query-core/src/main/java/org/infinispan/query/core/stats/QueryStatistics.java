package org.infinispan.query.core.stats;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * Exposes query statistics for a particular cache.
 *
 * @since 12.0
 */
public interface QueryStatistics extends JsonSerialization {
   /**
    * @return Number of queries executed in the local index.
    */
   long getLocalIndexedQueryCount();

   /**
    * @return Number of distributed indexed queries executed from the local node.
    */
   long getDistributedIndexedQueryCount();

   /**
    * @return Number of hybrid queries (two phase indexed and non-indexed) executed from the local node.
    */
   long getHybridQueryCount();

   /**
    * @return Number of non-indexed queries executed from the local node.
    */
   long getNonIndexedQueryCount();

   /**
    * @return The total time in nanoseconds of all indexed queries.
    */
   long getLocalIndexedQueryTotalTime();

   /**
    * @return The total time in nanoseconds of all distributed indexed queries.
    */
   long getDistributedIndexedQueryTotalTime();

   /**
    * @return The total time in nanoseconds for all hybrid queries.
    */
   long getHybridQueryTotalTime();

   /**
    * @return The total time in nanoseconds for all non-indexed queries.
    */
   long getNonIndexedQueryTotalTime();

   /**
    * @return The time in nanoseconds of the slowest indexed query.
    */
   long getLocalIndexedQueryMaxTime();

   /**
    * @return The time in nanoseconds of the slowest distributed indexed query.
    */
   long getDistributedIndexedQueryMaxTime();

   /**
    * @return The time in nanoseconds of the slowest hybrid query.
    */
   long getHybridQueryMaxTime();

   /**
    * @return The time in nanoseconds of the slowest non-indexed query.
    */
   long getNonIndexedQueryMaxTime();

   /**
    * @return The average time in nanoseconds of all indexed queries.
    */
   double getLocalIndexedQueryAvgTime();

   /**
    * @return The average time in nanoseconds of all distributed indexed queries.
    */
   double getDistributedIndexedQueryAvgTime();

   /**
    * @return The average time in nanoseconds of all hybrid indexed queries.
    */
   double getHybridQueryAvgTime();

   /**
    * @return The average time in nanoseconds of all non-indexed indexed queries.
    */
   double getNonIndexedQueryAvgTime();

   /**
    * @return The Ickle query string of the slowest indexed query.
    */
   String getSlowestLocalIndexedQuery();

   /**
    * @return The Ickle query string of the slowest distributed indexed query.
    */
   String getSlowestDistributedIndexedQuery();

   /**
    * @return The Ickle query string of the slowest hybrid query.
    */
   String getSlowestHybridQuery();

   /**
    * @return The Ickle query string of the slowest non-indexed query.
    */
   String getSlowestNonIndexedQuery();

   /**
    * @return The max time in nanoseconds to load entities from a Cache after an indexed query.
    */
   long getLoadMaxTime();

   /**
    * @return The average time in nanoseconds to load entities from a Cache after an indexed query.
    */
   double getLoadAvgTime();

   /**
    * @return The number of operations to load entities from a Cache after an indexed query.
    */
   long getLoadCount();

   /**
    * @return The total time to load entities from a Cache after an indexed query.
    */
   long getLoadTotalTime();

   /**
    * Clear all statistics.
    */
   void clear();

   /**
    * Merge with another {@link QueryStatistics}
    * @return self.
    */
   QueryStatistics merge(QueryStatistics other);

   /**
    * @return true if the Cache has statistics enabled.
    */
   boolean isEnabled();

}
