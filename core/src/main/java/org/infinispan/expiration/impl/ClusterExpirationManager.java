package org.infinispan.expiration.impl;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
   protected static final Log log = LogFactory.getLog(ClusterExpirationManager.class);
   protected static final boolean trace = log.isTraceEnabled();

   private ExecutorService asyncExecutor;
   private AdvancedCache<K, V> cache;

   @Inject
   public void inject(AdvancedCache<K, V> cache,
           @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR) ExecutorService asyncExecutor) {
      this.cache = cache;
      this.asyncExecutor = asyncExecutor;
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
                  if (ExpiryHelper.isExpiredMortal(e.getLifespan(), e.getCreated(), currentTimeMillis)) {
                     handleLifespanExpireEntry(e);
                  } else if (ExpiryHelper.isExpiredTransient(e.getMaxIdle(), e.getLastUsed(), currentTimeMillis)) {
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

   void handleLifespanExpireEntry(InternalCacheEntry<K, V> entry) {
      K key = entry.getKey();
      // The most used case will be a miss so no extra read before
      if (expiring.putIfAbsent(key, key) == null) {
         long lifespan = entry.getLifespan();
         if (trace) {
            log.tracef("Submitting expiration removal for key %s which had lifespan of %s", key, lifespan);
         }
         asyncExecutor.submit(() -> {
            try {
               removeExpired(key, entry.getValue(), lifespan);
            } finally {
               expiring.remove(key);
            }
         });
      }
   }

   private void removeExpired(K key, V value, Long lifespan) {
      cache.removeExpired(key, value, lifespan);
   }

   @Override
   public void handleInMemoryExpiration(InternalCacheEntry<K, V> entry, long currentTime) {
      // We need to synchronize on the entry since {@link InternalCacheEntry} locks the entry when doing an update
      // so we can see both the new value and the metadata
      synchronized (entry) {
         if (ExpiryHelper.isExpiredMortal(entry.getLifespan(), entry.getCreated(), currentTime)) {
            handleLifespanExpireEntry(entry);
         } else {
            super.handleInMemoryExpiration(entry, currentTime);
         }
      }
   }

   @Override
   public void handleInStoreExpiration(K key) {
      expiring.put(key, key);
      // Unfortunately stores don't pull the entry so we can't tell exactly why it expired and thus we have to remove
      // the entire value.  Unfortunately this could cause a concurrent write to be undone
      try {
         removeExpired(key, null, null);
      } finally {
         expiring.remove(key);
      }
   }

   @Override
   public void handleInStoreExpiration(MarshalledEntry<K, V> marshalledEntry) {
      K key = marshalledEntry.getKey();
      expiring.put(key, key);
      try {
         InternalMetadata metadata = marshalledEntry.getMetadata();
         removeExpired(key, marshalledEntry.getValue(), metadata.lifespan() == -1 ? null : metadata.lifespan());
      } finally {
         expiring.remove(key);
      }
   }
}
