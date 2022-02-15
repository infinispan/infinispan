package org.infinispan.client.hotrod;

import java.util.Map;

/**
 * Defines the possible list of statistics defined by the Hot Rod server.
 * Can be obtained through {@link RemoteCache#stats()}
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface ServerStatistics {

   /**
    * Number of seconds since Hot Rod started.
    */
   String TIME_SINCE_START = "timeSinceStart";

   /**
    * Number of entries currently in the Hot Rod server
    * @deprecated Since 14.0, please use {@link #APPROXIMATE_ENTRIES}
    */
   @Deprecated
   String CURRENT_NR_OF_ENTRIES = "currentNumberOfEntries";

   /**
    * Number of entries stored in the cache by the server that receives the request since the cache started running.
    * @deprecated Since 13.0, please use {@link #STORES} instead.
    */
   @Deprecated
   String TOTAL_NR_OF_ENTRIES = "totalNumberOfEntries";

   /**
    * Approximate current number of entry replicas in the cache on the server that receives the request.
    *
    * <p>Includes both entries in memory and in persistent storage.</p>
    */
   String APPROXIMATE_ENTRIES = "approximateEntries";

   /**
    * Approximate current number of entries for which the server that receives the request is the primary owner.
    *
    * <p>Includes both entries in memory and in persistent storage.</p>
    */
   String APPROXIMATE_ENTRIES_UNIQUE = "approximateEntriesUnique";

   /**
    * Number of entries stored in the cache by the server that receives the request since the cache started running.
    */
   String STORES = "stores";


   /**
    * Number of get operations.
    */
   String RETRIEVALS = "retrievals";

   /**
    * Number of get hits.
    */
   String HITS = "hits";

   /**
    * Number of get misses.
    */
   String MISSES = "misses";


   /**
    * Number of removal hits.
    */
   String REMOVE_HITS = "removeHits";

   /**
    * Number of removal misses.
    */
   String REMOVE_MISSES = "removeMisses";

   /**
    * Approximate current number of entry replicas currently in the cache cluster-wide.
    *
    * <p>Includes both entries in memory and in persistent storage.</p>
    */
   String CLUSTER_APPROXIMATE_ENTRIES = "globalApproximateEntries";

   /**
    * Approximate current number of unique entries in the cache cluster-wide.
    *
    * <p>Includes both entries in memory and in persistent storage.
    * Entries owned by multiple nodes are counted only once.</p>
    */
   String CLUSTER_APPROXIMATE_ENTRIES_UNIQUE = "globalApproximateEntriesUnique";

   Map<String, String> getStatsMap();

   String getStatistic(String statsName);

   Integer getIntStatistic(String statsName);
}
