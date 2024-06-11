package org.infinispan.client.hotrod.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.ServerStatistics;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
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
