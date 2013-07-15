package org.infinispan.stats;

import net.jcip.annotations.Immutable;

import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.InterceptorChain;

/**
 * StatsImpl.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class StatsImpl implements Stats {
   final long timeSinceStart;
   final int currentNumberOfEntries;
   final long totalNumberOfEntries;
   final long retrievals;
   final long stores;
   final long hits;
   final long misses;
   final long removeHits;
   final long removeMisses;
   final long evictions;
   final long averageReadTime;
   final long averageWriteTime;
   final long averageRemoveTime;
   final CacheMgmtInterceptor mgmtInterceptor;

   public StatsImpl(InterceptorChain chain) {
      mgmtInterceptor = (CacheMgmtInterceptor) chain
            .getInterceptorsWhichExtend(CacheMgmtInterceptor.class).get(0);

      if (mgmtInterceptor.getStatisticsEnabled()) {
         timeSinceStart = mgmtInterceptor.getElapsedTime();
         currentNumberOfEntries = mgmtInterceptor.getNumberOfEntries();
         totalNumberOfEntries = mgmtInterceptor.getStores();
         retrievals = mgmtInterceptor.getHits() + mgmtInterceptor.getMisses();
         stores = mgmtInterceptor.getStores();
         hits = mgmtInterceptor.getHits();
         misses = mgmtInterceptor.getMisses();
         removeHits = mgmtInterceptor.getRemoveHits();
         removeMisses = mgmtInterceptor.getRemoveMisses();
         evictions = mgmtInterceptor.getEvictions();
         averageReadTime = mgmtInterceptor.getAverageReadTime();
         averageWriteTime = mgmtInterceptor.getAverageWriteTime();
         averageRemoveTime = mgmtInterceptor.getAverageRemoveTime();
      } else {
         timeSinceStart = -1;
         currentNumberOfEntries = -1;
         totalNumberOfEntries = -1;
         retrievals = -1;
         stores = -1;
         hits = -1;
         misses = -1;
         removeHits = -1;
         removeMisses = -1;
         evictions = -1;
         averageReadTime = -1;
         averageWriteTime = -1;
         averageRemoveTime = -1;
      }
   }

   @Override
   public long getTimeSinceStart() {
      return timeSinceStart;
   }

   @Override
   public int getCurrentNumberOfEntries() {
      return currentNumberOfEntries;
   }

   @Override
   public long getTotalNumberOfEntries() {
      return totalNumberOfEntries;
   }

   @Override
   public long getRetrievals() {
      return retrievals;
   }

   @Override
   public long getStores() {
      return stores;
   }

   @Override
   public long getHits() {
      return hits;
   }

   @Override
   public long getMisses() {
      return misses;
   }

   @Override
   public long getRemoveHits() {
      return removeHits;
   }

   @Override
   public long getRemoveMisses() {
      return removeMisses;
   }

   @Override
   public long getEvictions() {
      return evictions;
   }

   @Override
   public long getAverageReadTime() {
      return averageReadTime;
   }

   @Override
   public long getAverageWriteTime() {
      return averageWriteTime;
   }

   @Override
   public long getAverageRemoveTime() {
      return averageRemoveTime;
   }

   @Override
   public void reset() {
      mgmtInterceptor.resetStatistics();
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      mgmtInterceptor.setStatisticsEnabled(enabled);
   }

}
