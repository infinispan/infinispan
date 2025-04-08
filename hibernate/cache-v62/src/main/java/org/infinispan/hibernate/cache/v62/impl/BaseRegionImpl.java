package org.infinispan.hibernate.cache.v62.impl;


import java.util.Map;
import java.util.function.Predicate;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.ExtendedStatisticsSupport;
import org.hibernate.cache.spi.Region;
import org.hibernate.stat.CacheRegionStatistics;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;

/**
 * Support for Infinispan {@link Region}s. Handles common "utility" methods for an underlying named
 * Cache. In other words, this implementation doesn't actually read or write data. Subclasses are
 * expected to provide core cache interaction appropriate to the semantics needed.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
abstract class BaseRegionImpl implements Region, InfinispanBaseRegion, ExtendedStatisticsSupport {

   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(BaseRegionImpl.class);

   private final String name;
   private final AdvancedCache localAndSkipLoadCache;
   final AdvancedCache cache;
   final InfinispanRegionFactory factory;

   private volatile long lastRegionInvalidation = Long.MIN_VALUE;
   private int invalidations = 0;
   Predicate<Map.Entry<Object, Object>> filter;

   /**
    * Base region constructor.
    *
    * @param cache instance for the region
    * @param name of the region
    * @param factory for this region
    */
   BaseRegionImpl(AdvancedCache cache, String name, InfinispanRegionFactory factory) {
      this.cache = cache;
      this.name = name;
      this.factory = factory;
      this.localAndSkipLoadCache = cache.withFlags(
            Flag.CACHE_MODE_LOCAL,
            Flag.FAIL_SILENTLY,
            Flag.ZERO_LOCK_ACQUISITION_TIMEOUT,
            Flag.SKIP_CACHE_LOAD
      );
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public long nextTimestamp() {
      return factory.nextTimestamp();
   }

   @Override
   public void destroy() throws CacheException {
      cache.stop();
   }

   /**
    * Checks if the region is valid for operations such as storing new data
    * in the region, or retrieving data from the region.
    *
    * @return true if the region is valid, false otherwise
    */
   @Override
   public boolean checkValid() {
      return lastRegionInvalidation != Long.MAX_VALUE;
   }

   @Override
   public void clear() {
      invalidateRegion();
   }

   @Override
   public void beginInvalidation() {
      if (log.isTraceEnabled()) {
         log.trace("Begin invalidating region: " + name);
      }
      synchronized (this) {
         lastRegionInvalidation = Long.MAX_VALUE;
         ++invalidations;
      }
      runInvalidation();
   }

   @Override
   public void endInvalidation() {
      synchronized (this) {
         if (--invalidations == 0) {
            lastRegionInvalidation = factory.nextTimestamp();
         }
      }
      if (log.isTraceEnabled()) {
         log.trace("End invalidating region: " + name);
      }
   }

   @Override
   public long getLastRegionInvalidation() {
      return lastRegionInvalidation;
   }

   @Override
   public AdvancedCache getCache() {
      return cache;
   }

   protected void runInvalidation() {
      log.tracef("Non-transactional, clear in one go");
      localAndSkipLoadCache.clear();
   }

   @Override
   public InfinispanRegionFactory getRegionFactory() {
      return factory;
   }

   @Override
   public long getElementCountInMemory() {
      if (filter == null) {
         return localAndSkipLoadCache.size();
      } else {
         return localAndSkipLoadCache.entrySet().stream().filter(filter).count();
      }
   }

   @Override
   public long getElementCountOnDisk() {
      return CacheRegionStatistics.NO_EXTENDED_STAT_SUPPORT_RETURN;
   }

   @Override
   public long getSizeInMemory() {
      return CacheRegionStatistics.NO_EXTENDED_STAT_SUPPORT_RETURN;
   }
}
