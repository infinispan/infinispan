package org.infinispan.jcache.remote;

import javax.cache.management.CacheStatisticsMXBean;

import java.util.concurrent.atomic.LongAdder;

/**
 * Statistics which reflect only cache interactions done though the
 * javax.cache API by the local client.
 */
public class LocalStatistics implements CacheStatisticsMXBean {
   private LongAdder cacheHits = new LongAdder();
   private LongAdder cacheMisses = new LongAdder();
   private LongAdder cacheGets = new LongAdder();
   private LongAdder cachePuts = new LongAdder();
   private LongAdder cacheRemovals = new LongAdder();
   private LongAdder cacheEvictions = new LongAdder();

   private float averageGetTime;
   private float averagePutTime;
   private float averageRemoveTime;

   public void incrementCacheHits() {
      cacheHits.add(1);
   }

   public void incrementCacheMisses() {
      cacheMisses.add(1);
   }

   public void incrementCacheGets() {
      cacheGets.add(1);
   }

   public void incrementCachePuts() {
      cachePuts.add(1);
   }

   public void incrementCacheRemovals() {
      cacheRemovals.add(1);
   }

   public void incrementCacheEvictions() {
      cacheEvictions.add(1);
   }

   @Override
   public void clear() {
      cacheHits.reset();
      cacheMisses.reset();
      cacheGets.reset();
      cachePuts.reset();
      cacheRemovals.reset();
      cacheEvictions.reset();
   }

   @Override
   public long getCacheHits() {
      return cacheHits.sum();
   }

   @Override
   public float getCacheHitPercentage() {
      long hits = cacheHits.sum();
      float total = hits + cacheMisses.sum();
      if (total <= 0)
         return 0;
      return (hits * 100f / total);
   }

   @Override
   public long getCacheMisses() {
      return cacheMisses.sum();
   }

   @Override
   public float getCacheMissPercentage() {
      long misses = cacheMisses.sum();
      float total = misses + cacheHits.sum();
      if (total <= 0)
         return 0;
      return (misses * 100f / total);
   }

   @Override
   public long getCacheGets() {
      return cacheGets.sum();
   }

   @Override
   public long getCachePuts() {
      return cachePuts.sum();
   }

   @Override
   public long getCacheRemovals() {
      return cacheRemovals.sum();
   }

   @Override
   public long getCacheEvictions() {
      return cacheEvictions.sum();
   }

   @Override
   public float getAverageGetTime() {
      return averageGetTime;
   }

   @Override
   public float getAveragePutTime() {
      return averagePutTime;
   }

   @Override
   public float getAverageRemoveTime() {
      return averageRemoveTime;
   }
}
