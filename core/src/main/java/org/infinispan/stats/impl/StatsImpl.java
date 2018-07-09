package org.infinispan.stats.impl;

import static org.infinispan.stats.impl.StatKeys.AVERAGE_READ_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_READ_TIME_NANOS;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_REMOVE_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_REMOVE_TIME_NANOS;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_WRITE_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_WRITE_TIME_NANOS;
import static org.infinispan.stats.impl.StatKeys.EVICTIONS;
import static org.infinispan.stats.impl.StatKeys.HITS;
import static org.infinispan.stats.impl.StatKeys.MISSES;
import static org.infinispan.stats.impl.StatKeys.NUMBER_OF_ENTRIES;
import static org.infinispan.stats.impl.StatKeys.NUMBER_OF_ENTRIES_IN_MEMORY;
import static org.infinispan.stats.impl.StatKeys.OFF_HEAP_MEMORY_USED;
import static org.infinispan.stats.impl.StatKeys.REMOVE_HITS;
import static org.infinispan.stats.impl.StatKeys.REMOVE_MISSES;
import static org.infinispan.stats.impl.StatKeys.REQUIRED_MIN_NODES;
import static org.infinispan.stats.impl.StatKeys.RETRIEVALS;
import static org.infinispan.stats.impl.StatKeys.STORES;
import static org.infinispan.stats.impl.StatKeys.TIME_SINCE_RESET;
import static org.infinispan.stats.impl.StatKeys.TIME_SINCE_START;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.stats.Stats;

import net.jcip.annotations.Immutable;

/**
 * StatsImpl.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class StatsImpl implements Stats {

   private static String[] Attributes = new String[]{TIME_SINCE_RESET, TIME_SINCE_START, NUMBER_OF_ENTRIES, NUMBER_OF_ENTRIES_IN_MEMORY,
         OFF_HEAP_MEMORY_USED, RETRIEVALS, STORES, HITS, MISSES, REMOVE_HITS, REMOVE_MISSES, EVICTIONS, AVERAGE_READ_TIME,
         AVERAGE_REMOVE_TIME, AVERAGE_WRITE_TIME, AVERAGE_READ_TIME_NANOS, AVERAGE_REMOVE_TIME_NANOS, AVERAGE_WRITE_TIME_NANOS, REQUIRED_MIN_NODES};

   private final Map<String, Long> statsMap = new HashMap<>();
   final CacheMgmtInterceptor mgmtInterceptor;
   final Stats source;

   public StatsImpl(AsyncInterceptorChain chain) {
      mgmtInterceptor = chain.findInterceptorExtending(CacheMgmtInterceptor.class);
      source = null;

      if (mgmtInterceptor.getStatisticsEnabled()) {
         statsMap.put(TIME_SINCE_RESET, mgmtInterceptor.getTimeSinceReset());
         statsMap.put(TIME_SINCE_START, mgmtInterceptor.getTimeSinceStart());
         statsMap.put(NUMBER_OF_ENTRIES, (long) mgmtInterceptor.getNumberOfEntries());
         statsMap.put(NUMBER_OF_ENTRIES_IN_MEMORY, (long) mgmtInterceptor.getNumberOfEntriesInMemory());
         statsMap.put(OFF_HEAP_MEMORY_USED, mgmtInterceptor.getOffHeapMemoryUsed());
         statsMap.put(RETRIEVALS, mgmtInterceptor.getHits() + mgmtInterceptor.getMisses());
         statsMap.put(STORES, mgmtInterceptor.getStores());
         statsMap.put(HITS, mgmtInterceptor.getHits());
         statsMap.put(MISSES, mgmtInterceptor.getMisses());
         statsMap.put(REMOVE_HITS, mgmtInterceptor.getRemoveHits());
         statsMap.put(REMOVE_MISSES, mgmtInterceptor.getRemoveMisses());
         statsMap.put(EVICTIONS, mgmtInterceptor.getEvictions());
         statsMap.put(AVERAGE_READ_TIME, mgmtInterceptor.getAverageReadTime());
         statsMap.put(AVERAGE_REMOVE_TIME, mgmtInterceptor.getAverageRemoveTime());
         statsMap.put(AVERAGE_WRITE_TIME, mgmtInterceptor.getAverageWriteTime());
         statsMap.put(AVERAGE_READ_TIME_NANOS, mgmtInterceptor.getAverageReadTimeNanos());
         statsMap.put(AVERAGE_REMOVE_TIME_NANOS, mgmtInterceptor.getAverageRemoveTimeNanos());
         statsMap.put(AVERAGE_WRITE_TIME_NANOS, mgmtInterceptor.getAverageWriteTimeNanos());
         statsMap.put(REQUIRED_MIN_NODES, (long) mgmtInterceptor.getRequiredMinimumNumberOfNodes());
      } else {
         for (String key : Attributes)
            statsMap.put(key, -1L);
      }
   }

   public StatsImpl(Stats other) {
      mgmtInterceptor = null;
      source = other;
      if (other != null) {
         statsMap.put(TIME_SINCE_RESET, other.getTimeSinceReset());
         statsMap.put(TIME_SINCE_START, other.getTimeSinceStart());
         statsMap.put(NUMBER_OF_ENTRIES, (long) other.getCurrentNumberOfEntries());
         statsMap.put(NUMBER_OF_ENTRIES_IN_MEMORY, (long) other.getCurrentNumberOfEntriesInMemory());
         statsMap.put(OFF_HEAP_MEMORY_USED, other.getOffHeapMemoryUsed());
         statsMap.put(RETRIEVALS, other.getHits() + other.getMisses());
         statsMap.put(STORES, other.getStores());
         statsMap.put(HITS, other.getHits());
         statsMap.put(MISSES, other.getMisses());
         statsMap.put(REMOVE_HITS, other.getRemoveHits());
         statsMap.put(REMOVE_MISSES, other.getRemoveMisses());
         statsMap.put(EVICTIONS, other.getEvictions());
         statsMap.put(AVERAGE_READ_TIME, other.getAverageReadTime());
         statsMap.put(AVERAGE_REMOVE_TIME, other.getAverageRemoveTime());
         statsMap.put(AVERAGE_WRITE_TIME, other.getAverageWriteTime());
         statsMap.put(AVERAGE_READ_TIME_NANOS, other.getAverageReadTimeNanos());
         statsMap.put(AVERAGE_REMOVE_TIME_NANOS, other.getAverageRemoveTimeNanos());
         statsMap.put(AVERAGE_WRITE_TIME_NANOS, other.getAverageWriteTimeNanos());
         statsMap.put(REQUIRED_MIN_NODES, (long) other.getRequiredMinimumNumberOfNodes());
      } else {
         for (String key : Attributes)
            statsMap.put(key, -1L);
      }
   }

   @Override
   public long getTimeSinceStart() {
      return statsMap.get(TIME_SINCE_START);
   }

   @Override
   public long getTimeSinceReset() {
      return statsMap.get(TIME_SINCE_RESET);
   }

   @Override
   public int getCurrentNumberOfEntries() {
      return Math.toIntExact(statsMap.get(NUMBER_OF_ENTRIES));
   }

   @Override
   public int getCurrentNumberOfEntriesInMemory() {
      return Math.toIntExact(statsMap.get(NUMBER_OF_ENTRIES_IN_MEMORY));
   }

   @Override
   public long getTotalNumberOfEntries() {
      return statsMap.get(STORES);
   }

   @Override
   public long getOffHeapMemoryUsed() {
      return statsMap.get(OFF_HEAP_MEMORY_USED);
   }

   @Override
   public long getRetrievals() {
      return statsMap.get(RETRIEVALS);
   }

   @Override
   public long getStores() {
      return statsMap.get(STORES);
   }

   @Override
   public long getHits() {
      return statsMap.get(HITS);
   }

   @Override
   public long getMisses() {
      return statsMap.get(MISSES);
   }

   @Override
   public long getRemoveHits() {
      return statsMap.get(REMOVE_HITS);
   }

   @Override
   public long getRemoveMisses() {
      return statsMap.get(REMOVE_MISSES);
   }

   @Override
   public long getEvictions() {
      return statsMap.get(EVICTIONS);
   }

   @Override
   public long getAverageReadTime() {
      return statsMap.get(AVERAGE_READ_TIME);
   }

   @Override
   public long getAverageWriteTime() {
      return statsMap.get(AVERAGE_WRITE_TIME);
   }

   @Override
   public long getAverageRemoveTime() {
      return statsMap.get(AVERAGE_REMOVE_TIME);
   }

   @Override
   public long getAverageReadTimeNanos() {
      return statsMap.get(AVERAGE_READ_TIME_NANOS);
   }

   @Override
   public long getAverageWriteTimeNanos() {
      return statsMap.get(AVERAGE_WRITE_TIME_NANOS);
   }

   @Override
   public long getAverageRemoveTimeNanos() {
      return statsMap.get(AVERAGE_REMOVE_TIME_NANOS);
   }

   @Override
   public int getRequiredMinimumNumberOfNodes() {
      return Math.toIntExact(statsMap.get(REQUIRED_MIN_NODES));
   }

   @Override
   public void reset() {
      if (mgmtInterceptor != null) {
         mgmtInterceptor.resetStatistics();
      } else if (source != null) {
         source.reset();
      }
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      if (mgmtInterceptor != null) {
         mgmtInterceptor.setStatisticsEnabled(enabled);
      } else if (source != null) {
         source.setStatisticsEnabled(enabled);
      }
   }
}
