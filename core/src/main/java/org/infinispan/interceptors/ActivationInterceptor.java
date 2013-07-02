package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;

public class ActivationInterceptor extends CacheLoaderInterceptor {

   private static final Log log = LogFactory.getLog(ActivationInterceptor.class);

   private Configuration cfg;
   private boolean isManualEviction;
   private ActivationManager activationManager;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(Configuration cfg, ActivationManager activationManager) {
      this.cfg = cfg;
      this.activationManager = activationManager;
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
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object retval = super.visitPutKeyValueCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());

      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = super.visitRemoveCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());
      return retval;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object retval = super.visitReplaceCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());
      return retval;
   }


   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object retval = super.visitPutMapCommand(ctx, command);
      removeFromStoreIfNeeded(command.getMap().keySet().toArray());
      return retval;
   }

   // read commands

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object retval = super.visitGetKeyValueCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());
      return retval;
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


