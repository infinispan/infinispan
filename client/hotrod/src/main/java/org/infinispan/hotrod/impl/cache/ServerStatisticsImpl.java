package org.infinispan.hotrod.impl.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 14.0
 */
public class ServerStatisticsImpl implements ServerStatistics {

   private final Map<String, String> stats = new HashMap<>();

   @Override
   public Map<String, String> getStatsMap() {
      return Collections.unmodifiableMap(stats);
   }

   @Override
   public String getStatistic(String statsName) {
      return stats.get(statsName);
   }

   public void addStats(String name, String value) {
      stats.put(name, value);
   }

   public int size() {
      return stats.size();
   }

   @Override
   public Integer getIntStatistic(String statsName) {
      String value = stats.get(statsName);
      return value == null ? null : Integer.parseInt(value);
   }
}
