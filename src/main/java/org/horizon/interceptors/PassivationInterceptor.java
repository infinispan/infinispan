package org.horizon.interceptors;

import org.horizon.commands.write.EvictCommand;
import org.horizon.config.ConfigurationException;
import org.horizon.container.DataContainer;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.interceptors.base.JmxStatsCommandInterceptor;
import org.horizon.jmx.annotations.ManagedAttribute;
import org.horizon.jmx.annotations.ManagedOperation;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;
import org.horizon.notifications.cachelistener.CacheNotifier;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes evicted entries back to the store on the way in through the CacheStore
 *
 * @since 4.0
 */
public class PassivationInterceptor extends JmxStatsCommandInterceptor {
   private final AtomicLong passivations = new AtomicLong(0);

   CacheStore cacheStore;
   CacheNotifier notifier;
   CacheLoaderManager cacheLoaderManager;
   DataContainer dataContainer;

   @Inject
   public void setDependencies(CacheNotifier notifier, CacheLoaderManager cacheLoaderManager, DataContainer dataContainer) {
      this.notifier = notifier;
      this.cacheLoaderManager = cacheLoaderManager;
      this.dataContainer = dataContainer;
   }

   @Start(priority = 15)
   public void start() {
      cacheStore = cacheLoaderManager == null ? null : cacheLoaderManager.getCacheStore();
      if (cacheStore == null)
         throw new ConfigurationException("passivation can only be used with a CacheLoader that implements CacheStore!");
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // notify listeners that this entry is about to be passivated
      Object key = command.getKey();
      notifier.notifyCacheEntryPassivated(key, true, ctx);
      log.trace("Passivating entry {0}", key);
      StoredEntry entryForStorage = dataContainer.createEntryForStorage(key);
      cacheStore.store(entryForStorage);
      notifier.notifyCacheEntryPassivated(key, false, ctx);
      if (getStatisticsEnabled() && entryForStorage != null) passivations.getAndIncrement();
      return invokeNextInterceptor(ctx, command);
   }

   @ManagedOperation
   public void resetStatistics() {
      passivations.set(0);
   }

   @ManagedAttribute(description = "Number of passivation events")
   public String getPassivations() {
      if (!getStatisticsEnabled()) return "N/A";
      return String.valueOf(passivations.get());
   }
}
