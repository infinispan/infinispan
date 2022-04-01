package org.infinispan.expiration.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public class ExpirationManagerImpl<K, V> implements InternalExpirationManager<K, V> {
   private static final Log log = LogFactory.getLog(ExpirationManagerImpl.class);

   @Inject @ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR)
   protected ScheduledExecutorService executor;
   @Inject protected Configuration configuration;
   @Inject protected PersistenceManager persistenceManager;
   @Inject protected ComponentRef<InternalDataContainer<K, V>> dataContainer;
   @Inject protected CacheNotifier<K, V> cacheNotifier;
   @Inject protected TimeService timeService;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected ComponentRef<AdvancedCache<K, V>> cacheRef;

   protected boolean enabled;
   protected String cacheName;
   protected AdvancedCache<K, V> cache;

   /**
    * This map is used for performance reasons.  Essentially when an expiration event should not be raised this
    * map should be populated first.  The main examples are if an expiration is about to occur for that key or the
    * key will be removed or updated.  In the latter case we don't want to send an expiration event and then a remove
    * event when we could do just the removal.
    */
   protected ConcurrentMap<K, CompletableFuture<Boolean>> expiring = new ConcurrentHashMap<>();
   protected ScheduledFuture<?> expirationTask;

   private final List<ExpirationConsumer<K, V>> listeners = new CopyOnWriteArrayList<>();

   @Start(priority = 55)
   // make sure this starts after the PersistenceManager
   public void start() {
      // first check if eviction is enabled!
      enabled = configuration.expiration().reaperEnabled();
      if (enabled) {
         // Set up the eviction timer task
         long expWakeUpInt = configuration.expiration().wakeUpInterval();
         if (expWakeUpInt <= 0) {
            CONTAINER.notStartingEvictionThread();
            enabled = false;
         } else {
            expirationTask = executor.scheduleWithFixedDelay(new ScheduledTask(),
                  expWakeUpInt, expWakeUpInt, TimeUnit.MILLISECONDS);
         }
      }
      // Data container entries are retrieved directly, so we don't need to worry about an encodings
      this.cache = AbstractDelegatingCache.unwrapCache(cacheRef.wired()).getAdvancedCache();
      this.cacheName = cache.getName();
   }

   @Override
   public void processExpiration() {
      long start = 0;
      if (!Thread.currentThread().isInterrupted()) {
         try {
            if (log.isTraceEnabled()) {
               log.trace("Purging data container of expired entries");
               start = timeService.time();
            }
            long currentTimeMillis = timeService.wallClockTime();
            for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = dataContainer.running().iteratorIncludingExpired();
                 purgeCandidates.hasNext();) {
               InternalCacheEntry<K, V> e = purgeCandidates.next();
               if (e.isExpired(currentTimeMillis)) {
                  entryExpiredInMemory(e, currentTimeMillis, false);
               }
            }
            if (log.isTraceEnabled()) {
               log.tracef("Purging data container completed in %s",
                          Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
            }
         } catch (Exception e) {
            CONTAINER.exceptionPurgingDataContainer(e);
         }
      }

      if (!Thread.currentThread().isInterrupted()) {
         CompletionStages.join(persistenceManager.purgeExpired());
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime,
         boolean hasLock) {
      // We ignore the return from this method. It is possible for the entry to no longer be expired, but this means
      // it was updated by another thread. In that case it is a completely valid value for it to be expired then not.
      // So for this we just tell the caller it was expired.
      dataContainer.running().compute(entry.getKey(), ((k, oldEntry, factory) -> {
         if (oldEntry != null) {
            synchronized (oldEntry) {
               if (oldEntry.isExpired(currentTime)) {
                  deleteFromStoresAndNotify(k, oldEntry.getValue(), oldEntry.getMetadata(), oldEntry.getInternalMetadata());
               } else {
                  return oldEntry;
               }
            }
         }
         return null;
      }));
      return CompletableFutures.completedTrue();
   }

   @Override
   public CompletionStage<Void> handleInStoreExpirationInternal(K key) {
      // Note since this is invoked without the actual key lock it is entirely possible for a remove to occur
      // concurrently before the data container lock is acquired and then the oldEntry below will be null causing an
      // expiration event to be generated that is extra
      return handleInStoreExpirationInternal(key, null, null, null);
   }

   @Override
   public CompletionStage<Void> handleInStoreExpirationInternal(final MarshallableEntry<K, V> marshalledEntry) {
      return handleInStoreExpirationInternal(marshalledEntry.getKey(), marshalledEntry.getValue(), marshalledEntry.getMetadata(), marshalledEntry.getInternalMetadata());
   }

   private CompletionStage<Void> handleInStoreExpirationInternal(K key, V value, Metadata metadata, PrivateMetadata privateMetadata) {
      dataContainer.running().compute(key, (oldKey, oldEntry, factory) -> {
         boolean shouldRemove = false;
         if (oldEntry == null) {
            shouldRemove = true;
            notify(key, value, metadata, privateMetadata);
         } else if (oldEntry.canExpire()) {
            long time = timeService.wallClockTime();
            if (oldEntry.isExpired(time)) {
               synchronized (oldEntry) {
                  // Even though we were provided marshalled entry - they may only provide metadata or value possibly
                  // so we have to check for null on either
                  shouldRemove = oldEntry.isExpired(time) && (metadata == null || oldEntry.getMetadata().equals(metadata)) &&
                        (value == null || value.equals(oldEntry.getValue()));
                  if (shouldRemove) {
                     notify(key, value, metadata, privateMetadata);
                  }
               }
            }
         }
         if (shouldRemove) {
            return null;
         }
         return oldEntry;
      });
      return CompletableFutures.completedNull();
   }

   /**
    * Deletes the key from the store as well as notifies the cache listeners of the expiration of the given key,
    * value, metadata combination.
    * <p>
    * This method must be invoked while holding data container lock for the given key to ensure events are ordered
    * properly.
    */
   private void deleteFromStoresAndNotify(K key, V value, Metadata metadata, PrivateMetadata privateMetadata) {
      listeners.forEach(l -> l.expired(key, value, metadata, privateMetadata)); //for internal use, assume non-blocking
      CompletionStages.join(CompletionStages.allOf(
            persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key), PersistenceManager.AccessMode.BOTH),
            cacheNotifier.notifyCacheEntryExpired(key, value, metadata, null)));
   }

   private void notify(K key, V value, Metadata metadata, PrivateMetadata privateMetadata) {
      listeners.forEach(l -> l.expired(key, value, metadata, privateMetadata)); //for internal use, assume non-blocking
      CompletionStages.join(cacheNotifier.notifyCacheEntryExpired(key, value, metadata, null));
   }

   @Override
   public CompletionStage<Boolean> handlePossibleExpiration(InternalCacheEntry<K, V> ice, int segment, boolean isWrite) {
      long currentTime = timeService.wallClockTime();
      if (ice.isExpired(currentTime)) {
         if (log.isTraceEnabled()) {
            log.tracef("Retrieved entry for key %s was expired locally, attempting expiration removal", ice.getKey());
         }
         CompletableFuture<Boolean> expiredStage = entryExpiredInMemory(ice, currentTime, isWrite);
         if (log.isTraceEnabled()) {
            expiredStage = expiredStage.thenApply(expired -> {
               if (expired == Boolean.FALSE) {
                  log.tracef("Retrieved entry for key %s was found to not be expired.", ice.getKey());
               } else {
                  log.tracef("Retrieved entry for key %s was confirmed to be expired.", ice.getKey());
               }
               return expired;
            });
         }
         return expiredStage;
      } else if (ice.canExpireMaxIdle()) {
         return checkExpiredMaxIdle(ice, segment, currentTime);
      }
      return CompletableFutures.completedFalse();
   }

   /**
    * Response is whether the value should be treated as expired. This is determined by if a value as able to be touched
    * or not, that is if it couldn't be touched - we assumed expired (as it was removed in some way).
    * @param entry the entry to check expiration and touch
    * @param segment the segment the entry maps to
    * @param currentTime the current time in milliseconds
    * @return whether the entry was expired or not
    */
   protected CompletionStage<Boolean> checkExpiredMaxIdle(InternalCacheEntry<?, ?> entry, int segment, long currentTime) {
      CompletionStage<Boolean> future = cache.touch(entry.getKey(), segment, true);
      if (CompletionStages.isCompletedSuccessfully(future)) {
         return CompletableFutures.booleanStage(!CompletionStages.join(future));
      }
      return future.thenApply(touched -> !touched);
   }

   @Stop(priority = 5)
   public void stop() {
      if (expirationTask != null) {
         expirationTask.cancel(true);
      }
   }

   @Override
   public void addInternalListener(ExpirationConsumer<K, V> consumer) {
      listeners.add(consumer);
   }

   @Override
   public void removeInternalListener(Object listener) {
      listeners.remove(listener);
   }

   class ScheduledTask implements Runnable {
      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, log.isTraceEnabled());
         try {
            processExpiration();
         } finally {
            LogFactory.popNDC(log.isTraceEnabled());
         }
      }
   }
}
