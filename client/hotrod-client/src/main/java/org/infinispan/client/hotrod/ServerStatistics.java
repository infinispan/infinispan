package org.infinispan.client.hotrod;

import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface ServerStatistics {

   /**
    * Number of seconds since Hot Rod started.
    */
   public static final String TIME_SINCE_START = "timeSinceStart";

   /**
    * Number of entries currently in the Hot Rod server
    */
   public static final String CURRENT_NR_OF_ENTRIES = "currentNumberOfEntries";

   /**
    * Number of entries stored in Hot Rod server.
    */
   public static final String TOTAL_NR_OF_ENTRIES = "totalNumberOfEntries";

   /**
    * Number of put operations.
    */
   public static final String STORES = "stores";


   /**
    * Number of get operations.
    */
   public static final String RETRIEVALS = "retrievals";

   /**
    * Number of get hits.
    */
   public static final String HITS = "hits";

   /**
    * Number of get misses.
    */
   public static final String MISSES = "misses";


   /**
    * Number of removal hits.
    */
   public static final String REMOVE_HITS = "removeHits";

   /**
    * Number of removal misses.
    */
   public static final String REMOVE_MISSES = "removeMisses";

   public Map<String, String> getStatsMap();

   public String getStatistic(String statsName);

   public Integer getIntStatistic(String statsName);
}
