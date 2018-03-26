package org.infinispan.hibernate.cache.main.timestamp;

import javax.transaction.Transaction;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.main.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.main.impl.BaseGeneralDataRegion;

public class TimestampsRegionImpl extends BaseGeneralDataRegion implements TimestampsRegion {

   private final AdvancedCache removeCache;
   private final AdvancedCache timestampsPutCache;

   public TimestampsRegionImpl(AdvancedCache cache, String name, InfinispanRegionFactory factory) {
      super(cache, name, factory);
      this.removeCache = Caches.ignoreReturnValuesCache(cache);

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

   @Override
   public Object get(SharedSessionContractImplementor session, Object key) throws CacheException {
      if (checkValid()) {
         return cache.get(key);
      }

      return null;
   }

   @Override
   public void put(SharedSessionContractImplementor session, Object key, Object value) throws CacheException {
      try {
         // We ensure ASYNC semantics (JBCACHE-1175) and make sure previous
         // value is not loaded from cache store cos it's not needed.
         timestampsPutCache.put(key, value);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   protected AdvancedCache getTimestampsPutCache(AdvancedCache cache) {
      return Caches.ignoreReturnValuesCache(cache, Flag.SKIP_LOCKING);
   }

   @Override
   public void evict(Object key) throws CacheException {
      // TODO Is this a valid operation on a timestamps cache?
      removeCache.remove(key);
   }

   @Override
   public void evictAll() throws CacheException {
      // TODO Is this a valid operation on a timestamps cache?
      final Transaction tx = suspend();
      try {
         // Invalidate the local region
         invalidateRegion();
      } finally {
         resume(tx);
      }
   }

}
