package org.infinispan.eviction;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

public class PassivationManagerImpl implements PassivationManager {

   CacheLoaderManager cacheLoaderManager;
   CacheNotifier notifier;
   CacheStore cacheStore;
   Configuration cfg;

   boolean statsEnabled = false;
   boolean enabled = false;
   private static final Log log = LogFactory.getLog(PassivationManagerImpl.class);
   private final AtomicLong passivations = new AtomicLong(0);
   private DataContainer container;
   private static final boolean trace = log.isTraceEnabled();

   @Inject
   public void inject(CacheLoaderManager cacheLoaderManager, CacheNotifier notifier, Configuration cfg, DataContainer container) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.notifier = notifier;
      this.cfg = cfg;
      this.container = container;
   }

   @Start(priority = 11)
   public void start() {
      enabled = cfg.getCacheLoaderManagerConfig().isPassivation();
      if (enabled) {
         cacheStore = cacheLoaderManager == null ? null : cacheLoaderManager.getCacheStore();
         if (cacheStore == null) {
            throw new ConfigurationException("passivation can only be used with a CacheLoader that implements CacheStore!");
         }

         enabled = cacheLoaderManager.isEnabled() && cacheLoaderManager.isUsingPassivation();
         statsEnabled = cfg.isExposeJmxStatistics();
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public void passivate(Object key, InternalCacheEntry entry, InvocationContext ctx) throws CacheLoaderException {
      if (enabled) {
         final Object value = entry != null ? entry.getValue() : null;
         // notify listeners that this entry is about to be passivated
         notifier.notifyCacheEntryPassivated(key, value, true, ctx);
         if (trace) log.trace("Passivating entry {0}", key);
         cacheStore.store(entry);
         notifier.notifyCacheEntryPassivated(key, value, false, ctx);
         if (statsEnabled && entry != null) {
            passivations.getAndIncrement();
         }
      }
   }

   @Stop(priority = 9)
   public void passivateAll() throws CacheLoaderException {
      if (enabled) {
         long start = System.currentTimeMillis();
         log.info("Passivating all entries to disk");
         for (InternalCacheEntry e : container) {
            if (trace) log.trace("Passivating {0}", e.getKey());
            cacheStore.store(e);
         }
         log.info("Passivated {0} entries in {1}", container.size(), Util.prettyPrintTime(System.currentTimeMillis() - start));
      }
   }

   public long getPassivationCount() {
      return passivations.get();
   }

   public void resetPassivationCount() {
      passivations.set(0L);
   }
}
