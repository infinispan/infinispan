package org.infinispan.hibernate.cache.v51.query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.engine.spi.SessionImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.access.SessionAccess;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.InvocationAfterCompletion;
import org.infinispan.hibernate.cache.v51.InfinispanRegionFactory;
import org.infinispan.hibernate.cache.v51.impl.BaseTransactionalDataRegion;
import org.infinispan.transaction.TransactionMode;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

public final class QueryResultsRegionImpl extends BaseTransactionalDataRegion implements QueryResultsRegion {

   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( QueryResultsRegionImpl.class );
   private static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();
   private final AdvancedCache evictCache;
   private final AdvancedCache putCache;
   private final AdvancedCache getCache;
   private final ConcurrentMap<Object, Map> transactionContext = new ConcurrentHashMap<>();
   private final boolean putCacheRequiresTransaction;

   public QueryResultsRegionImpl(AdvancedCache cache, String name, TransactionManager transactionManager, InfinispanRegionFactory factory) {
      super(cache, name, transactionManager, null, factory, null );
      // If Infinispan is using INVALIDATION for query cache, we don't want to propagate changes.
      // We use the Timestamps cache to manage invalidation
      final boolean localOnly = Caches.isInvalidationCache(cache);

      this.evictCache = localOnly ? Caches.localCache(cache) : cache;

      this.putCache = localOnly ?
            Caches.failSilentWriteCache(cache, Flag.CACHE_MODE_LOCAL ) :
            Caches.failSilentWriteCache(cache);

      this.getCache = Caches.failSilentReadCache(cache);

      TransactionConfiguration transactionConfiguration = this.putCache.getCacheConfiguration().transaction();
      boolean transactional = transactionConfiguration.transactionMode() != TransactionMode.NON_TRANSACTIONAL;
      this.putCacheRequiresTransaction = transactional && !transactionConfiguration.autoCommit();
      // Since we execute the query update explicitly form transaction synchronization, the putCache does not need
      // to be transactional anymore (it had to be in the past to prevent revealing uncommitted changes).
      if (transactional) {
         log.useNonTransactionalQueryCache();
      }

   }

   @Override
   public Object get(SessionImplementor session, Object key) throws CacheException {
      return getItem(session, key);
   }

   @Override
   public void put(SessionImplementor session, Object key, Object value) throws CacheException {
      putItem(session, key, value);
   }

   @Override
   protected boolean isRegionAccessStrategyEnabled() {
      return false;
   }

   public void evict(Object key) throws CacheException {
      for (Map map : transactionContext.values()) {
         map.remove(key);
      }
      evictCache.remove( key );
   }

   public void evictAll() throws CacheException {
      transactionContext.clear();
      final Transaction tx = suspend();
      try {
         // Invalidate the local region and then go remote
         invalidateRegion();
         Caches.broadcastEvictAll(cache);
      } finally {
         resume( tx );
      }
   }

   public Object getItem(Object session, Object key) throws CacheException {
      if ( !checkValid() ) {
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
         result = getCache.get( key );
      }
      return result;
   }

   @SuppressWarnings("unchecked")
   public void putItem(Object session, Object key, Object value) throws CacheException {
      if ( checkValid() ) {
         // See HHH-7898: Even with FAIL_SILENTLY flag, failure to write in transaction
         // fails the whole transaction. It is an Infinispan quirk that cannot be fixed
         // ISPN-5356 tracks that. This is because if the transaction continued the
         // value could be committed on backup owners, including the failed operation,
         // and the result would not be consistent.
         SessionAccess.TransactionCoordinatorAccess tc = SESSION_ACCESS.getTransactionCoordinator(session);
         if (tc != null && tc.isJoined()) {
            tc.registerLocalSynchronization(new PostTransactionQueryUpdate(tc, session, key, value));
            // no need to synchronize as the transaction will be accessed by only one thread
            Map map = transactionContext.get(session);
            if (map == null) {
               transactionContext.put(session, map = new HashMap());
            }
            map.put(key, value);
            return;
         }
         // Here we don't want to suspend the tx. If we do:
         // 1) We might be caching query results that reflect uncommitted
         // changes. No tx == no WL on cache node, so other threads
         // can prematurely see those query results
         // 2) No tx == immediate replication. More overhead, plus we
         // spread issue #1 above around the cluster

         // Add a zero (or quite low) timeout option so we don't block.
         // Ignore any TimeoutException. Basically we forego caching the
         // query result in order to avoid blocking.
         // Reads are done with suspended tx, so they should not hold the
         // lock for long.  Not caching the query result is OK, since
         // any subsequent read will just see the old result with its
         // out-of-date timestamp; that result will be discarded and the
         // db query performed again.
         putCache.put( key, value );
      }
   }

   private class PostTransactionQueryUpdate extends InvocationAfterCompletion {
      private final Object session;
      private final Object key;
      private final Object value;

      public PostTransactionQueryUpdate(SessionAccess.TransactionCoordinatorAccess tc, Object session, Object key, Object value) {
         super(tc, putCacheRequiresTransaction);
         this.session = session;
         this.key = key;
         this.value = value;
      }

      @Override
      public void afterCompletion(int status) {
         transactionContext.remove(session);
         super.afterCompletion(status);
      }

      @Override
      protected void invoke(boolean success) {
         if (success) {
            putCache.put(key, value);
         }
      }
   }
}
