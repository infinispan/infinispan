package org.infinispan.hibernate.cache.v53.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.access.SessionAccess;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.InvocationAfterCompletion;
import org.infinispan.hibernate.cache.v53.InfinispanRegionFactory;
import org.infinispan.transaction.TransactionMode;

public final class QueryResultsRegionImpl extends BaseRegionImpl implements QueryResultsRegion {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( QueryResultsRegionImpl.class );
   private static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();

   private final AdvancedCache putCache;
   private final AdvancedCache getCache;
   private final ConcurrentMap<Object, Map> transactionContext = new ConcurrentHashMap<>();
   private final boolean putCacheRequiresTransaction;

   /**
    * Query region constructor
    *  @param cache instance to store queries
    * @param name of the query region
    * @param factory for the query region
    */
   public QueryResultsRegionImpl(AdvancedCache cache, String name, InfinispanRegionFactory factory) {
      super(cache, name, factory);
      // If Infinispan is using INVALIDATION for query cache, we don't want to propagate changes.
      // We use the Timestamps cache to manage invalidation
      final boolean localOnly = Caches.isInvalidationCache( cache );

      this.putCache = localOnly ?
            Caches.failSilentWriteCache( cache, Flag.CACHE_MODE_LOCAL ) :
            Caches.failSilentWriteCache( cache );

      this.getCache = Caches.failSilentReadCache( cache );

      TransactionConfiguration transactionConfiguration = putCache.getCacheConfiguration().transaction();
      boolean transactional = transactionConfiguration.transactionMode() != TransactionMode.NON_TRANSACTIONAL;
      this.putCacheRequiresTransaction = transactional && !transactionConfiguration.autoCommit();
      // Since we execute the query update explicitly form transaction synchronization, the putCache does not need
      // to be transactional anymore (it had to be in the past to prevent revealing uncommitted changes).
      if (transactional) {
         log.useNonTransactionalQueryCache();
      }

   }

   @Override
   public void clear() throws CacheException {
      transactionContext.clear();
      // Invalidate the local region and then go remote
      invalidateRegion();
      Caches.broadcastEvictAll( cache );
   }

   @Override
   public Object getFromCache(Object key, SharedSessionContractImplementor session) {
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
      SessionAccess.TransactionCoordinatorAccess tc = SESSION_ACCESS.getTransactionCoordinator(session);
      if (tc != null && tc.isJoined()) {
         tc.registerLocalSynchronization(new QueryResultsRegionImpl.PostTransactionQueryUpdate(tc, session, key, value));
         // no need to synchronize as the transaction will be accessed by only one thread
         transactionContext.computeIfAbsent(session, k -> new HashMap()).put(key, value);
      } else {
         putCache.put(key, value);
      }
   }

   private class PostTransactionQueryUpdate extends InvocationAfterCompletion {
      private final Object session;
      private final Object key;
      private final Object value;

      PostTransactionQueryUpdate(SessionAccess.TransactionCoordinatorAccess tc, Object session, Object key, Object value) {
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
