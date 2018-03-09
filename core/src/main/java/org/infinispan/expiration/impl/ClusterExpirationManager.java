package org.infinispan.expiration.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

/**
 * Allows for cluster based expirations to occur.  This provides guarantees that when an entry is expired that it will
 * expire that entry across the entire cluster at once.  This requires obtaining the lock for said entry before
 * expiration is performed.  Since expiration can occur without holding onto the lock it is possible for an expiration
 * to occur immediately after a value has been updated.  This can cause a premature expiration to occur.  Attempts
 * are made to prevent this by using the expired entry's value and lifespan to limit this expiration so it only happens
 * in a smaller amount of cases.
 * <p>
 * Cache stores however do not supply the value or metadata information which means if an entry is purged from the cache
 * store that it will forcibly remove the value even if a concurrent write updated it just before.  This will be
 * addressed by future SPI changes to the cache store.
 * @param <K>
 * @param <V>
 */
@ThreadSafe
public class ClusterExpirationManager<K, V> extends ExpirationManagerImpl<K, V> {
   private static final Log log = LogFactory.getLog(ClusterExpirationManager.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   private ExecutorService asyncExecutor;
   @Inject private AdvancedCache<K, V> cache;
   private boolean needTransaction;

   public ExecutorService getAsyncExecutor() {
      return asyncExecutor;
   }

   @Override
   public void start() {
      super.start();
      // Data container entries are retrieved directly, so we don't need to worry about an encodings
      this.cache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache();
      needTransaction = configuration.transaction().transactionMode().isTransactional();
   }

   @Override
   public void processExpiration() {
      long start = 0;
      if (!Thread.currentThread().isInterrupted()) {
         try {
            if (trace) {
               log.trace("Purging data container of expired entries");
               start = timeService.time();
            }
            long currentTimeMillis = timeService.wallClockTime();
            for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = dataContainer.iteratorIncludingExpired();
                 purgeCandidates.hasNext();) {
               InternalCacheEntry<K, V> e = purgeCandidates.next();
               if (e.canExpire()) {
                  // Have to synchronize on the entry to make sure we see the value and metadata at the same time
                  boolean expiredMortal;
                  boolean expiredTransient;
                  V value;
                  long lifespan;
                  synchronized (e) {
                     value = e.getValue();
                     lifespan = e.getLifespan();
                     expiredMortal = ExpiryHelper.isExpiredMortal(lifespan, e.getCreated(), currentTimeMillis);
                     expiredTransient = ExpiryHelper.isExpiredTransient(e.getMaxIdle(), e.getLastUsed(), currentTimeMillis);
                  }
                  if (expiredMortal) {
                     handleLifespanExpireEntry(e.getKey(), value, lifespan, true);
                  } else if (expiredTransient) {
                     super.handleInMemoryExpiration(e, currentTimeMillis);
                  }
               }
            }
            if (trace) {
               log.tracef("Purging data container completed in %s",
                       Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
            }
         } catch (Exception e) {
            log.exceptionPurgingDataContainer(e);
         }
      }

      if (!Thread.currentThread().isInterrupted()) {
         persistenceManager.purgeExpired();
      }
   }

   void handleLifespanExpireEntry(K key, V value, long lifespan, boolean sync) {
      // The most used case will be a miss so no extra read before
      if (expiring.putIfAbsent(key, key) == null) {
         if (trace) {
            log.tracef("Submitting expiration removal for key %s which had lifespan of %s", toStr(key), lifespan);
         }
         Runnable runnable = () -> {
            try {
               removeExpired(key, value, lifespan);
            } finally {
               expiring.remove(key);
            }
         };
         if (sync) {
            runnable.run();
         } else {
            asyncExecutor.submit(runnable);
         }
      }
   }

   private void removeExpired(K key, V value, Long lifespan) {
      if (needTransaction) {
         TransactionManager tm = cache.getTransactionManager();
         try {
            Transaction tx = tm.suspend();
            try {
               tm.begin();
               cache.removeExpired(key, value, lifespan);
            } catch (NotSupportedException | SystemException e) {
               tm.rollback();
               throw e;
            } finally {
               tm.commit();
            }
            if (tx != null) {
               tm.resume(tx);
            }
         } catch (RollbackException | NotSupportedException | SystemException | HeuristicMixedException |
               HeuristicRollbackException | InvalidTransactionException e) {
            throw new CacheException(e);
         }
      } else {
         cache.removeExpired(key, value, lifespan);
      }
   }

   @Override
   public void handleInMemoryExpiration(InternalCacheEntry<K, V> entry, long currentTime) {
      // We need to synchronize on the entry since {@link InternalEntryFactoryImpl} locks the entry when doing an update
      // so we can see both the new value and the metadata
      boolean expiredMortal;
      V value;
      long lifespan;
      synchronized (entry) {
         value = entry.getValue();
         lifespan = entry.getLifespan();
         expiredMortal = ExpiryHelper.isExpiredMortal(lifespan, entry.getCreated(), currentTime);
      }
      if (expiredMortal) {
         handleLifespanExpireEntry(entry.getKey(), value, lifespan, false);
      } else {
         super.handleInMemoryExpiration(entry, currentTime);
      }
   }

   @Override
   public void handleInStoreExpiration(K key) {
      if (expiring.putIfAbsent(key, key) == null) {
         // Unfortunately stores don't pull the entry so we can't tell exactly why it expired and thus we have to remove
         // the entire value.  Unfortunately this could cause a concurrent write to be undone
         try {
            removeExpired(key, null, null);
         } finally {
            expiring.remove(key);
         }
      }
   }

   @Override
   public void handleInStoreExpiration(MarshalledEntry<K, V> marshalledEntry) {
      K key = marshalledEntry.getKey();
      if (expiring.putIfAbsent(key, key) == null) {
         try {
            InternalMetadata metadata = marshalledEntry.getMetadata();
            removeExpired(key, marshalledEntry.getValue(), metadata.lifespan() == -1 ? null : metadata.lifespan());
         } finally {
            expiring.remove(key);
         }
      }
   }
}
