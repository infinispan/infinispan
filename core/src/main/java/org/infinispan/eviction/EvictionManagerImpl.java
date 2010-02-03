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
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CountDownLatch;
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
   volatile CountDownLatch startLatch = new CountDownLatch(1);

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR) ScheduledExecutorService executor,
                          Configuration configuration, Cache cache, DataContainer dataContainer, CacheLoaderManager cacheLoaderManager) {
      this.executor = executor;
      this.configuration = configuration;
      this.cache = cache;
      this.dataContainer = dataContainer;
      this.cacheLoaderManager = cacheLoaderManager;
   }

   @Start(priority = 55)
   // make sure this starts after the CacheLoaderManager
   public void start() {
      // first check if eviction is enabled!
      enabled = configuration.getEvictionStrategy() != EvictionStrategy.NONE;
      if (enabled) {
         maxEntries = configuration.getEvictionMaxEntries();
         if (cacheLoaderManager != null && cacheLoaderManager.isEnabled())
            cacheStore = cacheLoaderManager.getCacheStore();
         // Set up the eviction timer task
         if (configuration.getEvictionWakeUpInterval() <= 0) {
            log.info("wakeUpInterval is <= 0, not starting eviction thread");
         } else {
            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(), configuration.getEvictionWakeUpInterval(),
                                                           configuration.getEvictionWakeUpInterval(), TimeUnit.MILLISECONDS);
         }
      }
      startLatch.countDown();
   }

   public void processEviction() {
      try {
         startLatch.await();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }

      if (!enabled) return;

      long start = 0;
      try {
         if (trace) {
            log.trace("Purging data container of expired entries");
            start = System.currentTimeMillis();
         }
         dataContainer.purgeExpired();
         if (trace)
            log.trace("Purging data container completed in {0}", Util.prettyPrintTime(System.currentTimeMillis() - start));
      } catch (Exception e) {
         log.warn("Caught exception purging data container!", e);
      }

      if (cacheStore != null) {
         try {
            if (trace) {
               log.trace("Purging cache store of expired entries");
               start = System.currentTimeMillis();
            }
            cacheStore.purgeExpired();
            if (trace)
               log.trace("Purging cache store completed in {0}", Util.prettyPrintTime(System.currentTimeMillis() - start));
         } catch (Exception e) {
            log.warn("Caught exception purging cache store!", e);
         }
      }

      // finally iterate through data container if too big
      int dcsz = dataContainer.size();
      if (dcsz > maxEntries) {
         if (trace) {
            log.trace("Data container is larger than maxEntries, size is {0}.  Evicting...", dcsz);
            start = System.currentTimeMillis();
         }
         for (InternalCacheEntry ice : dataContainer) {
            Object k = ice.getKey();
            try {
               dcsz = dataContainer.size();
               if (dcsz > maxEntries) {
                  if (trace) log.trace("Attempting to evict key [{0}]", k);
                  try {
                     cache.evict(k);
                  } catch (TimeoutException te) {
                     log.trace("Unable to evict key {0} due to a timeout.  Moving on to next possible evictable entry.", k);
                  }
               } else {
                  if (trace) log.trace("Evicted enough entries");
                  break;
               }
            } catch (Exception e) {
               log.warn("Caught exception when iterating through data container.  Current entry is under key [{0}]", e, k);
            }
         }
         if (trace)
            log.trace("Eviction process completed in {0}", Util.prettyPrintTime(System.currentTimeMillis() - start));
      } else {
         if (trace) log.trace("Data container is smaller than or equal to the maxEntries; not doing anything");
      }
   }

   public boolean isEnabled() {
      return enabled;
   }

   @Stop(priority = 5)
   public void stop() {
      startLatch = new CountDownLatch(1);
      if (evictionTask != null) evictionTask.cancel(true);
   }

   class ScheduledTask implements Runnable {
      public void run() {
         processEviction();
      }
   }
}
