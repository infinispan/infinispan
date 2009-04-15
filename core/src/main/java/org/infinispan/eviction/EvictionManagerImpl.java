package org.infinispan.eviction;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.loader.CacheStore;
import org.infinispan.loader.CacheLoaderManager;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class EvictionManagerImpl implements EvictionManager {
   private static final Log log = LogFactory.getLog(EvictionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   ScheduledFuture evictionTask;

   // components to be injected
   ScheduledExecutorService executor;
   Configuration configuration;
   Cache cache;
   CacheLoaderManager cacheLoaderManager;
   DataContainer dataContainer;
   CacheStore cacheStore;
   boolean enabled;
   int maxEntries;

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR) ScheduledExecutorService executor,
                          Configuration configuration, Cache cache, DataContainer dataContainer, CacheLoaderManager cacheLoaderManager) {
      this.executor = executor;
      this.configuration = configuration;
      this.cache = cache;
      this.dataContainer = dataContainer;
      this.cacheLoaderManager = cacheLoaderManager;
   }

   @Start (priority = 55) // make sure this starts after the CacheLoaderManager
   public void start() {
      // first check if eviction is enabled!
      enabled = configuration.getEvictionStrategy() != EvictionStrategy.NONE;
      if (enabled) {
         maxEntries = configuration.getEvictionMaxEntries();
         if (cacheLoaderManager != null && cacheLoaderManager.isEnabled()) cacheStore = cacheLoaderManager.getCacheStore();
         // Set up the eviction timer task
         if (configuration.getEvictionWakeUpInterval() <= 0) {
            log.info("wakeUpInterval is <= 0, not starting eviction thread");
         } else {
            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(), configuration.getEvictionWakeUpInterval(),
                                                           configuration.getEvictionWakeUpInterval(), TimeUnit.MILLISECONDS);
         }
      }
   }

   public void processEviction() {
      try {
         if (trace) log.trace("Purging data container of expired entries");
         dataContainer.purgeExpired();
      } catch (Exception e) {
         log.warn("Caught exception purging data container!", e);
      }

      if (cacheStore != null) {
         try {
            if (trace) log.trace("Purging cache store of expired entries");
            cacheStore.purgeExpired();
         } catch (Exception e) {
            log.warn("Caught exception purging cache store!", e);
         }
      }

      // finally iterate through data container if too big
      int dcsz = dataContainer.size();
      if (dcsz > maxEntries) {
         if (trace) log.trace("Data container is larger than maxEntries, size is {0}.  Evicting...", dcsz);
         for (InternalCacheEntry ice: dataContainer) {
            dcsz = dataContainer.size();
            if (dcsz > maxEntries) {
               cache.evict(ice.getKey());
            } else {
               if (trace) log.trace("Evicted enough entries");
               break;
            }
         }
      } else {
         if (trace) log.trace("Data container is smaller than or equal to the maxEntries; not doing anything");
      }
   }

   public boolean isEnabled() {
      return enabled;
   }

   @Stop(priority = 5)
   public void stop() {
      if (evictionTask != null) evictionTask.cancel(true);
   }

   class ScheduledTask implements Runnable {
      public void run() {
         processEviction();
      }
   }
}
