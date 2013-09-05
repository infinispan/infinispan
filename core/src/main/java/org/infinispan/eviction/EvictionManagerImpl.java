package org.infinispan.eviction;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class EvictionManagerImpl implements EvictionManager {
   private static final Log log = LogFactory.getLog(EvictionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   ScheduledFuture <?> evictionTask;

   // components to be injected
   private ScheduledExecutorService executor;
   private Configuration configuration;
   private PersistenceManager persistenceManager;
   private DataContainer dataContainer;
   private CacheNotifier cacheNotifier;
   private TimeService timeService;
   private boolean enabled;
   private String cacheName;

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR)
         ScheduledExecutorService executor, Cache cache, Configuration cfg, DataContainer dataContainer,
         PersistenceManager persistenceManager, CacheNotifier cacheNotifier, TimeService timeService) {
      initialize(executor, cache.getName(), cfg, dataContainer,
                 persistenceManager, cacheNotifier, timeService);
   }

   void initialize(ScheduledExecutorService executor,
            String cacheName, Configuration cfg, DataContainer dataContainer,
            PersistenceManager persistenceManager, CacheNotifier cacheNotifier, TimeService timeService) {
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
         } else {
            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(),
                  expWakeUpInt, expWakeUpInt, TimeUnit.MILLISECONDS);
         }
      }
   }

   @Override
   public void processEviction() {
      long start = 0;
      if (!Thread.currentThread().isInterrupted()) {
         try {
            if (trace) {
               log.trace("Purging data container of expired entries");
               start = timeService.time();
            }
            dataContainer.purgeExpired();
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

   @Stop(priority = 5)
   public void stop() {
      if (evictionTask != null) {
         evictionTask.cancel(true);
      }
   }

   class ScheduledTask implements Runnable {
      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            processEviction();
         } finally {
            LogFactory.popNDC(trace);
         }
      }
   }

   @Override
   public void onEntryEviction(Map<Object, InternalCacheEntry> evicted) {
      // don't reuse the threadlocal context as we don't want to include eviction
      // operations in any ongoing transaction, nor be affected by flags
      // especially see ISPN-1154: it's illegal to acquire locks in a committing transaction
      InvocationContext ctx = ImmutableContext.INSTANCE;
      // This is important because we make no external guarantees on the thread
      // that will execute this code, so it could be the user thread, or could
      // be the eviction thread.
      // However, when a user calls cache.evict(), you do want to carry over the
      // contextual information, hence it makes sense for the notifyCacheEntriesEvicted()
      // call to carry on taking an InvocationContext object.
      cacheNotifier.notifyCacheEntriesEvicted(evicted.values(), ctx, null);
   }
}
