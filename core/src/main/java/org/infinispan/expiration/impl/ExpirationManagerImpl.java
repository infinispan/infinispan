package org.infinispan.expiration.impl;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ExpirationManagerImpl<K, V> implements InternalExpirationManager<K, V> {
   private static final Log log = LogFactory.getLog(ExpirationManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject @ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR)
   protected ScheduledExecutorService executor;
   @Inject protected Configuration configuration;
   @Inject protected PersistenceManager persistenceManager;
   @Inject protected ComponentRef<InternalDataContainer<K, V>> dataContainer;
   @Inject protected CacheNotifier<K, V> cacheNotifier;
   @Inject protected TimeService timeService;
   @Inject protected KeyPartitioner keyPartitioner;

   protected boolean enabled;
   protected String cacheName;

   /**
    * This map is used for performance reasons.  Essentially when an expiration event should not be raised this
    * map should be populated first.  The main examples are if an expiration is about to occur for that key or the
    * key will be removed or updated.  In the latter case we don't want to send an expiration event and then a remove
    * event when we could do just the removal.
    */
   protected ConcurrentMap<K, Object> expiring = new ConcurrentHashMap<>();
   protected ScheduledFuture<?> expirationTask;

   // used only for testing
   void initialize(ScheduledExecutorService executor, String cacheName, Configuration cfg) {
      this.executor = executor;
      this.configuration = cfg;
      this.cacheName = cacheName;
   }

   @Start(priority = 55)
   // make sure this starts after the PersistenceManager
   public void start() {
      // first check if eviction is enabled!
      enabled = configuration.expiration().reaperEnabled();
      if (enabled) {
         // Set up the eviction timer task
         long expWakeUpInt = configuration.expiration().wakeUpInterval();
         if (expWakeUpInt <= 0) {
            log.notStartingEvictionThread();
            enabled = false;
         } else {
            expirationTask = executor.scheduleWithFixedDelay(new ScheduledTask(),
                  expWakeUpInt, expWakeUpInt, TimeUnit.MILLISECONDS);
         }
      }
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
            for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = dataContainer.running().iteratorIncludingExpired();
                 purgeCandidates.hasNext();) {
               InternalCacheEntry<K, V> e = purgeCandidates.next();
               if (e.isExpired(currentTimeMillis)) {
                  entryExpiredInMemory(e, currentTimeMillis);
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

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime) {
      // We ignore the return from this method. It is possible for the entry to no longer be expired, but this means
      // it was updated by another thread. In that case it is a completely valid value for it to be expired then not.
      // So for this we just tell the caller it was expired.
      dataContainer.running().compute(entry.getKey(), ((k, oldEntry, factory) -> {
         if (oldEntry != null) {
            synchronized (oldEntry) {
               if (oldEntry.isExpired(currentTime)) {
                  deleteFromStoresAndNotify(k, oldEntry.getValue(), oldEntry.getMetadata());
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
   public CompletableFuture<Boolean> entryExpiredInMemoryFromIteration(InternalCacheEntry<K, V> entry, long currentTime) {
      // Local we just remove the entry as we see them
      return entryExpiredInMemory(entry, currentTime);
   }

   @Override
   public void handleInMemoryExpiration(InternalCacheEntry<K, V> entry, long currentTime) {
      // Just invoke the new method and join
      entryExpiredInMemory(entry, currentTime).join();
   }

   @Override
   public void handleInStoreExpiration(K key) {
      // Note since this is invoked without the actual key lock it is entirely possible for a remove to occur
      // concurrently before the data container lock is acquired and then the oldEntry below will be null causing an
      // expiration event to be generated that is extra
      handleInStoreExpiration(key, null, null);
   }

   @Override
   public void handleInStoreExpiration(final MarshalledEntry<K, V> marshalledEntry) {
      handleInStoreExpiration(marshalledEntry.getKey(), marshalledEntry.getValue(), marshalledEntry.getMetadata());
   }

   private void handleInStoreExpiration(K key, V value, Metadata metadata) {
      dataContainer.running().compute(key, (oldKey, oldEntry, factory) -> {
         boolean shouldRemove = false;
         if (oldEntry == null) {
            shouldRemove = true;
            deleteFromStoresAndNotify(key, value, metadata);
         } else if (oldEntry.canExpire()) {
            long time = timeService.time();
            if (oldEntry.isExpired(time)) {
               synchronized (oldEntry) {
                  if (oldEntry.isExpired(time)) {
                     // Even though we were provided marshalled entry - they may only provide metadata or value possibly
                     // so we have to check for null on either
                     if (shouldRemove = (metadata == null || oldEntry.getMetadata().equals(metadata)) &&
                             (value == null || value.equals(oldEntry.getValue()))) {
                        deleteFromStoresAndNotify(key, value, metadata);
                     }
                  }
               }
            }
         }
         if (shouldRemove) {
            return null;
         }
         return oldEntry;
      });
   }

   /**
    * Deletes the key from the store as well as notifies the cache listeners of the expiration of the given key,
    * value, metadata combination.
    * @param key
    * @param value
    * @param metadata
    */
   private void deleteFromStoresAndNotify(K key, V value, Metadata metadata) {
      deleteFromStores(key);
      if (cacheNotifier != null) {
         // To guarantee ordering of events this must be done on the entry, so that another write cannot be
         // done at the same time
         cacheNotifier.notifyCacheEntryExpired(key, value, metadata, null);
      }
   }

   private void deleteFromStores(K key) {
      // We have to delete from shared stores as well to make sure there are not multiple expiration events
      persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key), PersistenceManager.AccessMode.BOTH);
   }

   protected Long localLastAccess(Object key, Object value, int segment) {
      InternalCacheEntry ice = dataContainer.running().peek(segment, key);
      if (ice != null && (value == null || value.equals(ice.getValue())) &&
            !ice.isExpired(timeService.wallClockTime())) {
         return ice.getLastUsed();
      }
      return null;
   }

   @Override
   public CompletableFuture<Long> retrieveLastAccess(Object key, Object value, int segment) {
      Long lastAccess = localLastAccess(key, value, segment);
      if (lastAccess != null) {
         return CompletableFuture.completedFuture(lastAccess);
      }
      return CompletableFutures.completedNull();
   }

   @Stop(priority = 5)
   public void stop() {
      if (expirationTask != null) {
         expirationTask.cancel(true);
      }
   }

   class ScheduledTask implements Runnable {
      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            processExpiration();
         } finally {
            LogFactory.popNDC(trace);
         }
      }
   }
}
