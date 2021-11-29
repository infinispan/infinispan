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
    * Number of entries stored in Hot Rod server
    * since the server started running.
    * @deprecated Since 13.0, please use {@link #STORES} instead.
    */
   @Deprecated
   String TOTAL_NR_OF_ENTRIES = "totalNumberOfEntries";

   /**
    * Approximate number of entry copies currently in the server cache, including persistence.
    *
    * Only entries local to the server receiving the request are included.
    */
   String APPROXIMATE_ENTRIES = "approximateEntries";

   /**
    * Number of unique entries currently stored in server cache, including persistence.
    *
    * Only entries primary-owned by the server receiving the request are included.
    */
   String APPROXIMATE_ENTRIES_UNIQUE = "approximateEntriesUnique";

   /**
    * Number of entries ever stored.
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
    * Approximate number of entry copies currently in the server cache cluster-wide, including persistence.
    *
    * Entries owned by multiple nodes are counted once for each owner.
    */
   String CLUSTER_APPROXIMATE_ENTRIES = "globalApproximateEntries";

   /**
    * Number of unique entries currently stored in server cache cluster-wide, including persistence.
    *
    * Entries owned by multiple nodes are counted only once.
    */
   String CLUSTER_APPROXIMATE_ENTRIES_UNIQUE = "globalApproximateEntriesUnique";

   Map<String, String> getStatsMap();

   String getStatistic(String statsName);

   Integer getIntStatistic(String statsName);
}
