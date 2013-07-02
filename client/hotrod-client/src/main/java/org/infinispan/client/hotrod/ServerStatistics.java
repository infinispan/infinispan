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
    */
   String CURRENT_NR_OF_ENTRIES = "currentNumberOfEntries";

   /**
    * Number of entries stored in Hot Rod server
    * since the server started running.
    */
   String TOTAL_NR_OF_ENTRIES = "totalNumberOfEntries";

   /**
    * Number of put operations.
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

   Map<String, String> getStatsMap();

   String getStatistic(String statsName);

   Integer getIntStatistic(String statsName);
}
