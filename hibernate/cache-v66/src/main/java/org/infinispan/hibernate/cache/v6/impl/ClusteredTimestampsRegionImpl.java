package org.infinispan.hibernate.cache.v6.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.cache.CacheException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.v6.InfinispanRegionFactory;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

@Listener
public class ClusteredTimestampsRegionImpl extends TimestampsRegionImpl {
   /**
    * Maintains a local (authoritative) cache of timestamps along with the
    * replicated cache held in Infinispan. It listens for changes in the
    * cache and updates the local cache accordingly. This approach allows
    * timestamp changes to be replicated asynchronously.
    */
   private final Map localCache = new ConcurrentHashMap();

   /**
    * Clustered timestamps region constructor.
    *
    * @param cache instance to store update timestamps
    * @param name of the update timestamps region
    * @param factory for the update timestamps region
    */
   public ClusteredTimestampsRegionImpl(
         AdvancedCache cache,
         String name, InfinispanRegionFactory factory) {
      super(cache, name, factory);
      cache.addListener(this);
      populateLocalCache();
   }

   @Override
   protected AdvancedCache getTimestampsPutCache(AdvancedCache cache) {
      return Caches.asyncWriteCache(cache, Flag.SKIP_LOCKING);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object getFromCache(Object key, SharedSessionContractImplementor session) {
      Object value = localCache.get(key);

      if (value == null && checkValid()) {
         value = cache.get(key);

         if (value != null) {
            localCache.put(key, value);
         }
      }
      return value;
   }

   @Override
   @SuppressWarnings("unchecked")
   public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
      // Local write: always replace. Hibernate issues preInvalidate (now+timeout) then
      // invalidate (now) in guaranteed sequential order on this node, so the second write
      // must win unconditionally. Using Math.max here would permanently retain the larger
      // preInvalidate timestamp and keep queries stale for the full timeout period.
      localCache.put(key, value);
      super.putIntoCache(key, value, session);
   }

   @Override
   public void invalidateRegion() {
      // Invalidate first
      super.invalidateRegion();
      localCache.clear();
   }

   @Override
   public void destroy() throws CacheException {
      localCache.clear();
      cache.removeListener(this);
      super.destroy();
   }

   /**
    * Brings all data from the distributed cache into our local cache.
    */
   private void populateLocalCache() {
      try (CloseableIterator iterator = cache.keySet().iterator()) {
         while (iterator.hasNext()) {
            getFromCache(iterator.next(), null);
         }
      }
   }

   /**
    * Monitors cache events and updates the local cache
    *
    * @param event The event
    */
   @CacheEntryModified
   @SuppressWarnings({"unused", "unchecked"})
   public void nodeModified(CacheEntryModifiedEvent event) {
      if (!event.isPre()) {
         if (event.isOriginLocal()) {
            // Already handled synchronously in putIntoCache; skip here to avoid the
            // Math.max merge racing with a subsequent local write on this same node.
            return;
         }
         // Remote event: use Math.max to guard against out-of-order async delivery
         // from another cluster node where a stale write could arrive after a newer one.
         updateLocalCache(event.getKey(), event.getValue());
      }
   }

   /**
    * Monitors cache events and updates the local cache
    *
    * @param event The event
    */
   @CacheEntryRemoved
   @SuppressWarnings("unused")
   public void nodeRemoved(CacheEntryRemovedEvent event) {
      if (event.isPre()) {
         return;
      }
      localCache.remove(event.getKey());
   }

   // Used only for remote events (isOriginLocal() == false) where async replication
   // can deliver writes out of order across nodes. Math.max prevents a stale remote
   // write from downgrading a more recent timestamp already in the local cache.
   // TODO: with recent Infinispan we should access the cache directly
   private void updateLocalCache(Object key, Object value) {
      localCache.compute(key, (k, v) -> {
         if (v instanceof Number && value instanceof Number) {
            return Math.max(((Number) v).longValue(), ((Number) value).longValue());
         } else {
            return value;
         }
      });
   }
}
