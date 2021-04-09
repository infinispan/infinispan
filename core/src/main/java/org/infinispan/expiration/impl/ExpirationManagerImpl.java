package org.infinispan.expiration.impl;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
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
   @Inject protected ComponentRef<CommandsFactory> cf;
   @Inject protected ComponentRef<AsyncInterceptorChain> invokerRef;
   @Inject protected ComponentRegistry componentRegistry;

   protected boolean enabled;
   protected String cacheName;

   /**
    * This map is used for performance reasons.  Essentially when an expiration event should not be raised this
    * map should be populated first.  The main examples are if an expiration is about to occur for that key or the
    * key will be removed or updated.  In the latter case we don't want to send an expiration event and then a remove
    * event when we could do just the removal.
    */
   protected ConcurrentMap<K, CompletableFuture<Boolean>> expiring = new ConcurrentHashMap<>();
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
                  entryExpiredInMemory(e, currentTimeMillis, false);
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
   public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime,
         boolean hasLock) {
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
   public boolean entryExpiredInMemoryFromIteration(InternalCacheEntry<K, V> entry, long currentTime) {
      // Local we just remove the entry as we see them
      return CompletionStages.join(entryExpiredInMemory(entry, currentTime, false));
   }

   private void entryExpiredInMemorySync(InternalCacheEntry<K, V> entry, long currentTime) {
      dataContainer.running().compute(entry.getKey(), ((k, oldEntry, factory) -> {
         if (oldEntry != null) {
            synchronized (oldEntry) {
               if (oldEntry.isExpired(currentTime)) {
                  deleteFromStoresAndNotifySync(k, oldEntry.getValue(), oldEntry.getMetadata());
               } else {
                  return oldEntry;
               }
            }
         }
         return null;
      }));
   }

   /**
    * Same as {@link #deleteFromStoresAndNotify(Object, Object, Metadata)} except that the store removal is done
    * synchronously - this means this method <b>MUST</b> be invoked in the blocking thread pool
    * @param key
    * @param value
    * @param metadata
    */
   private void deleteFromStoresAndNotifySync(K key, V value, Metadata metadata) {
      persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key), PersistenceManager.AccessMode.BOTH);
      cacheNotifier.notifyCacheEntryExpired(key, value, metadata, null);
   }

   @Override
   public void handleInMemoryExpiration(InternalCacheEntry<K, V> entry, long currentTime) {
      // Just invoke the new method and join
      entryExpiredInMemory(entry, currentTime, false).join();
   }

   @Override
   public CompletionStage<Void> handleInStoreExpirationInternal(K key) {
      // Note since this is invoked without the actual key lock it is entirely possible for a remove to occur
      // concurrently before the data container lock is acquired and then the oldEntry below will be null causing an
      // expiration event to be generated that is extra
      return handleInStoreExpirationInternal(key, null, null);
   }

   @Override
   public CompletionStage<Void> handleInStoreExpirationInternal(final MarshalledEntry<K, V> marshalledEntry) {
      return handleInStoreExpirationInternal(marshalledEntry.getKey(), marshalledEntry.getValue(), marshalledEntry.getMetadata());
   }

   private CompletionStage<Void> handleInStoreExpirationInternal(K key, V value, Metadata metadata) {
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
                        // TODO: this is blocking! - this needs to be fixed in https://issues.redhat.com/browse/ISPN-10377
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
      return CompletableFutures.completedNull();
   }

   /**
    * Deletes the key from the store as well as notifies the cache listeners of the expiration of the given key,
    * value, metadata combination.
    * <p>
    * This method must be invoked while holding data container lock for the given key to ensure events are ordered
    * properly.
    * @param key
    * @param value
    * @param metadata
    */
   private void deleteFromStoresAndNotify(K key, V value, Metadata metadata) {
         persistenceManager.deleteFromAllStores(key, keyPartitioner.getSegment(key), PersistenceManager.AccessMode.BOTH);
         cacheNotifier.notifyCacheEntryExpired(key, value, metadata, null);
   }

   @Override
   public CompletionStage<Boolean> handlePossibleExpiration(InternalCacheEntry<K, V> ice, int segment, boolean isWrite) {
      long currentTime = timeService.wallClockTime();
      if (ice.isExpired(currentTime)) {
         if (trace) {
            log.tracef("Retrieved entry for key %s was expired locally, attempting expiration removal", ice.getKey());
         }
         CompletableFuture<Boolean> expiredStage = entryExpiredInMemory(ice, currentTime, isWrite);
         if (trace) {
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
      } else if (!isWrite && ice.canExpireMaxIdle()) {
         return touchEntryAndReturnIfExpired(ice, segment);
      }
      return CompletableFutures.completedFalse();
   }

   /**
    * Response is whether the value should be treated as expired - thus if both local and remote were able to touch
    * then the value is not expired. Note this is different then the touch command's response normally as that mentions
    * if it was touched or not
    * @param entry
    * @param segment
    * @return
    */
   protected CompletionStage<Boolean> touchEntryAndReturnIfExpired(InternalCacheEntry entry, int segment) {
      TouchCommand touchCommand = cf.running().buildTouchCommand(entry.getKey(), segment);
      touchCommand.init(dataContainer.running(), timeService, configuration, null);
      CompletableFuture<Boolean> future = (CompletableFuture) touchCommand.invokeAsync();
      if (CompletionStages.isCompletedSuccessfully(future)) {
         return future.join() ? CompletableFutures.completedFalse() : CompletableFutures.completedTrue();
      }
      return future.thenApply(touched -> !touched);
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
