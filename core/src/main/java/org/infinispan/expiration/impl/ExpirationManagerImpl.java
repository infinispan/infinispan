package org.infinispan.expiration.impl;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class ExpirationManagerImpl<K, V> implements ExpirationManager<K, V> {
   private static final Log log = LogFactory.getLog(ExpirationManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   ScheduledFuture <?> expirationTask;

   // components to be injected
   private ScheduledExecutorService executor;
   private Configuration configuration;
   private PersistenceManager persistenceManager;
   private DataContainer<K, V> dataContainer;
   private CacheNotifier<K, V> cacheNotifier;
   private TimeService timeService;
   private boolean enabled;
   private String cacheName;

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR)
         ScheduledExecutorService executor, Cache<K, V> cache, Configuration cfg, DataContainer<K, V> dataContainer,
         PersistenceManager persistenceManager, CacheNotifier<K, V> cacheNotifier, TimeService timeService) {
      initialize(executor, cache.getName(), cfg, dataContainer,
                 persistenceManager, cacheNotifier, timeService);
   }

   void initialize(ScheduledExecutorService executor, String cacheName, Configuration cfg,
           DataContainer<K, V> dataContainer, PersistenceManager persistenceManager, CacheNotifier<K, V> cacheNotifier,
           TimeService timeService) {
      this.executor = executor;
      this.configuration = cfg;
      this.cacheName = cacheName;
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.cacheNotifier = cacheNotifier;
      this.timeService = timeService;
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
            for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = dataContainer.iteratorIncludingExpired();
                 purgeCandidates.hasNext();) {
               InternalCacheEntry<K, V> e = purgeCandidates.next();
               if (e.isExpired(currentTimeMillis)) {
                  handleInMemoryExpiration(e);
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
   public void handleInMemoryExpiration(InternalCacheEntry<K, V> entry) {
      dataContainer.compute(entry.getKey(), ((k, oldEntry, factory) -> {
         if (entry == oldEntry) {
            // We have to delete from shared stores as well to make sure there are not multiple expiration events
            persistenceManager.deleteFromAllStores(k, PersistenceManager.AccessMode.BOTH);
            if (cacheNotifier != null) {
               cacheNotifier.notifyCacheEntryExpired(k, entry.getValue(), entry.getMetadata());
            }
            return null;
         }
         return oldEntry;
      }));
   }

   @Override
   public void handleInStoreExpiration(K key) {
      // Note since this is invoked without the actual key lock it is entirely possible for a remove to occur
      // concurrently before the data container lock is acquired and then the oldEntry below will be null causing an
      // expiration event to be generated that is extra
      dataContainer.compute(key, ((k, oldEntry, factory) -> {
         if (oldEntry == null || (oldEntry.canExpire() && oldEntry.isExpired(timeService.time()))) {
            // We have to delete from shared stores as well to make sure there are not multiple expiration events
            persistenceManager.deleteFromAllStores(key, PersistenceManager.AccessMode.BOTH);
            if (cacheNotifier != null) {
               V value;
               Metadata metadata;
               if (oldEntry != null) {
                  value = oldEntry.getValue();
                  metadata = oldEntry.getMetadata();
               } else {
                  value = null;
                  metadata = null;
               }
               cacheNotifier.notifyCacheEntryExpired(k, value, metadata);
            }
            return null;
         }
         return oldEntry;
      }));
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
