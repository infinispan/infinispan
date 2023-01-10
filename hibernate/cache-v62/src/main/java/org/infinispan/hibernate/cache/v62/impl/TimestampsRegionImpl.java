package org.infinispan.hibernate.cache.v62.impl;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;

public class TimestampsRegionImpl extends BaseRegionImpl implements TimestampsRegion {

   private final AdvancedCache timestampsPutCache;

   public TimestampsRegionImpl(AdvancedCache cache, String name, InfinispanRegionFactory factory) {
      super(cache, name, factory);

      // Skip locking when updating timestamps to provide better performance
      // under highly concurrent insert scenarios, where update timestamps
      // for an entity/collection type are constantly updated, creating
      // contention.
      //
      // The worst it can happen is that an earlier an earlier timestamp
      // (i.e. ts=1) will override a later on (i.e. ts=2), so it means that
      // in highly concurrent environments, queries might be considered stale
      // earlier in time. The upside is that inserts/updates are way faster
      // in local set ups.
      this.timestampsPutCache = getTimestampsPutCache(cache);
   }

   protected AdvancedCache getTimestampsPutCache(AdvancedCache cache) {
      return Caches.ignoreReturnValuesCache(cache, Flag.SKIP_LOCKING);
   }

   @Override
   public Object getFromCache(Object key, SharedSessionContractImplementor session) {
      if (checkValid()) {
         return cache.get(key);
      }
      return null;
   }

   @Override
   public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
      try {
         timestampsPutCache.put(key, value);
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
