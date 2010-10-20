package org.infinispan.eviction;

import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationManagerImpl implements PassivationManager {

   CacheLoaderManager cacheLoaderManager;
   CacheNotifier notifier;
   CacheStore cacheStore;
   Configuration cfg;

   boolean statsEnabled = false;
   boolean enabled = false;
   private static final Log log = LogFactory.getLog(PassivationManagerImpl.class);
   private final AtomicLong passivations = new AtomicLong(0);

   @Inject
   public void inject(CacheLoaderManager cacheLoaderManager, CacheNotifier notifier, Configuration cfg) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.notifier = notifier;
      this.cfg = cfg;
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
         log.trace("Passivating entry {0}", key);
         cacheStore.store(entry);
         notifier.notifyCacheEntryPassivated(key, value, false, ctx);
         if (statsEnabled && entry != null) {
            passivations.getAndIncrement();
         }
      }
   }

   public long getPassivationCount() {
      return passivations.get();
   }

   public void resetPassivationCount() {
      passivations.set(0L);
   }
}
