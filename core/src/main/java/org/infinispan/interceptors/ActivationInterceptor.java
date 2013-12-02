package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;

public class ActivationInterceptor extends CacheLoaderInterceptor {

   private static final Log log = LogFactory.getLog(ActivationInterceptor.class);

   private Configuration cfg;
   private boolean isManualEviction;
   private ActivationManager activationManager;
   private ClusteringDependentLogic cdl;
   private DataContainer dataContainer;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(Configuration cfg, ActivationManager activationManager, ClusteringDependentLogic cdl,
                      DataContainer dataContainer) {
      this.cfg = cfg;
      this.activationManager = activationManager;
      this.cdl = cdl;
      this.dataContainer = dataContainer;
   }

   @Start(priority = 15)
   @SuppressWarnings("unused")
   public void start() {
      // Treat caches configured with manual eviction differently.
      // These caches require activation at the interceptor level.
      EvictionConfiguration evictCfg = cfg.eviction();
      isManualEviction = evictCfg.strategy() == EvictionStrategy.NONE
            || evictCfg.maxEntries() < 0;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // Load the keys for our map into the data container that we are using removing them from the store.
      // This way when we overwrite the values on commit they won't be in loader and if we rollback they won't be
      // in the loader either but will be in data container at least
      for (Object key : command.getAffectedKeys()) {
         loadIfNeeded(ctx, key, false, command);
      }
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   protected Boolean loadIfNeeded(InvocationContext ctx, Object key, boolean isRetrieval, FlagAffectedCommand cmd) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry != null && !entry.isNull() && entry.getValue() != null) {
         //the entry is already in the context. Avoid look in the cache loader.
         return null;
      }
      try {
         while (!cdl.lock(key, false));
         InternalCacheEntry ice = dataContainer.get(key);
         if (ice != null) {
            if (cmd instanceof ApplyDeltaCommand) {
               ctx.putLookedUpEntry(key, ice);
               entryFactory.wrapEntryForDelta(ctx, key, ((ApplyDeltaCommand) cmd).getDelta());
            } else {
               entryFactory.wrapEntryForPut(ctx, key, ice, false, cmd, false);
            }
            return null;
         }
         Boolean loaded = super.loadIfNeeded(ctx, key, isRetrieval, cmd);
         if (loaded == Boolean.TRUE) {
            if (enabled && isManualEviction) {
               // check if value was loaded
               CacheEntry e = ctx.lookupEntry(key);
               // We have to commit this before we remove from the store so there isn't a gap where the entry isn't visible
               // This should always be true given that loaded was true, but just sanity check
               if (e != null && e.isLoaded()) {
                  cdl.commitEntry(e, null, cmd, ctx);
                  removeFromStoreIfNeeded(key);
               }
            }
         }
         return loaded;
      } finally {
         cdl.unlock(key);
      }
   }

   @Override
   protected void sendNotification(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand cmd) {
      super.sendNotification(key, value, pre, ctx, cmd);
      notifier.notifyCacheEntryActivated(key, value, pre, ctx, cmd);
   }

   private void removeFromStoreIfNeeded(Object... keys) {
      if (enabled && isManualEviction) {
         if (log.isTraceEnabled())
            log.tracef("Remove from store keys=%s, if needed", Arrays.toString(keys));

         for (Object key: keys)
            activationManager.activate(key);
      }
   }

}


