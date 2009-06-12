package org.infinispan.interceptors;

import org.infinispan.commands.write.EvictCommand;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes evicted entries back to the store on the way in through the CacheStore
 *
 * @since 4.0
 */
@MBean(objectName = "Passivation", description = "Component that handles passivating entries to a CacheStore on eviction.")
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
      InternalCacheEntry entryForStorage = dataContainer.get(key);
      cacheStore.store(entryForStorage);
      notifier.notifyCacheEntryPassivated(key, false, ctx);
      if (getStatisticsEnabled() && entryForStorage != null) passivations.getAndIncrement();
      return invokeNextInterceptor(ctx, command);
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   public void resetStatistics() {
      passivations.set(0);
   }

   @ManagedAttribute(description = "Number of passivation events")
   public String getPassivations() {
      if (!getStatisticsEnabled()) return "N/A";
      return String.valueOf(passivations.get());
   }
}
