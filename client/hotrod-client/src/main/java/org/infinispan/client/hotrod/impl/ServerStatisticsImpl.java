package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.ServerStatistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ServerStatisticsImpl implements ServerStatistics {

   public static final Set<String> supportedStatNames;

   private Map<String, Number> stats = new HashMap<String, Number>();

   static {
      Set<String> keys = new HashSet<String>();
      keys.add(CURRENT_NR_OF_ENTRIES);
      keys.add(HITS);
      keys.add(MISSES);
      keys.add(REMOVE_HITS);
      keys.add(REMOVE_MISSES);
      keys.add(RETRIEVALS);
      keys.add(TOTAL_NR_OF_ENTRIES);
      keys.add(TIME_SINCE_START);
      supportedStatNames = Collections.unmodifiableSet(keys);
   }

   @Override
   public Map<String, Number> getStatsMap() {
      return Collections.unmodifiableMap(stats);
   }

   @Override
   public Number getStats(String statsName) {
      return stats.get(statsName);
   }

   public void addStats(String name, Number value) {
      if (!supportedStatNames.contains(name)) {
         throw new IllegalArgumentException("Unknown stat: '" + name);
      }
      stats.put(name, value);
   }
}
