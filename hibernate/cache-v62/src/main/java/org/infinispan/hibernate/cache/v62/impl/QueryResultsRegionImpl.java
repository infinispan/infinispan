package org.infinispan.hibernate.cache.v62.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.v62.InfinispanRegionFactory;

public final class QueryResultsRegionImpl extends BaseRegionImpl implements QueryResultsRegion {
   private final AdvancedCache<Object, Object> putCache;
   private final AdvancedCache<Object, Object> getCache;
   private final ConcurrentMap<Object, Map> transactionContext = new ConcurrentHashMap<>();

   /**
    * Query region constructor
    *
    * @param cache instance to store queries
    * @param name of the query region
    * @param factory for the query region
    */
   public QueryResultsRegionImpl(AdvancedCache cache, String name, InfinispanRegionFactory factory) {
      super(cache, name, factory);
      // If Infinispan is using INVALIDATION for query cache, we don't want to propagate changes.
      // We use the Timestamps cache to manage invalidation
      final boolean localOnly = Caches.isInvalidationCache(cache);

      this.putCache = localOnly ?
            Caches.failSilentWriteCache(cache, Flag.CACHE_MODE_LOCAL) :
            Caches.failSilentWriteCache(cache);

      this.getCache = Caches.failSilentReadCache(cache);
   }

   @Override
   public void clear() throws CacheException {
      transactionContext.clear();
      // Invalidate the local region and then go remote
      invalidateRegion();
      Caches.broadcastEvictAll(cache);
   }

   @Override
   public Object getFromCache(Object key, SharedSessionContractImplementor session) {
      if (!checkValid()) {
         return null;
      }

      // In Infinispan get doesn't acquire any locks, so no need to suspend the tx.
      // In the past, when get operations acquired locks, suspending the tx was a way
      // to avoid holding locks that would prevent updates.
      // Add a zero (or low) timeout option so we don't block
      // waiting for tx's that did a put to commit
      Object result = null;
      Map map = transactionContext.get(session);
      if (map != null) {
         result = map.get(key);
      }
      if (result == null) {
         result = getCache.get(key);
      }
      return result;
   }

   @Override
   public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
      if (!checkValid()) {
         return;
      }
      // See HHH-7898: Even with FAIL_SILENTLY flag, failure to write in transaction
      // fails the whole transaction. It is an Infinispan quirk that cannot be fixed
      // ISPN-5356 tracks that. This is because if the transaction continued the
      // value could be committed on backup owners, including the failed operation,
      // and the result would not be consistent.
      Sync sync = (Sync) session.getCacheTransactionSynchronization();
      if (sync != null && session.isTransactionInProgress()) {
         sync.registerAfterCommit(new PostTransactionQueryUpdate(session, key, value));
         // no need to synchronize as the transaction will be accessed by only one thread
         transactionContext.computeIfAbsent(session, k -> new HashMap<>()).put(key, value);
      } else {
         putCache.put(key, value);
      }
   }

   private class PostTransactionQueryUpdate implements Invocation {
      private final Object session;
      private final Object key;
      private final Object value;

      PostTransactionQueryUpdate(Object session, Object key, Object value) {
         this.session = session;
         this.key = key;
         this.value = value;
      }

      @Override
      public CompletableFuture<Object> invoke(boolean success) {
         transactionContext.remove(session);
         if (success) {
            return putCache.putAsync(key, value);
         } else {
            return null;
         }
      }
   }
}
