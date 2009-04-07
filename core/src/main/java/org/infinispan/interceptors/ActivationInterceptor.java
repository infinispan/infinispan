package org.infinispan.interceptors;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loader.CacheLoaderException;
import org.infinispan.loader.CacheStore;

import java.util.concurrent.atomic.AtomicLong;

public class ActivationInterceptor extends CacheLoaderInterceptor {

   private AtomicLong activations = new AtomicLong(0);
   private CacheStore store;

   @Start(priority = 15)
   public void setCacheStore() {
      store = clm == null ? null : clm.getCacheStore();
      if (store == null)
         throw new ConfigurationException("passivation can only be used with a CacheLoader that implements CacheStore!");
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object retval = super.visitPutKeyValueCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = super.visitRemoveCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object retval = super.visitReplaceCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }


   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object retval = super.visitPutMapCommand(ctx, command);
      removeFromStore(command.getMap().keySet().toArray());
      return retval;
   }

   // read commands

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object retval = super.visitGetKeyValueCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }

   private void removeFromStore(Object... keys) throws CacheLoaderException {
      for (Object k : keys) {
         if (store.remove(k) && getStatisticsEnabled()) {
            activations.incrementAndGet();
         }
      }
   }

   @Override
   protected void sendNotification(Object key, boolean pre, InvocationContext ctx) {
      super.sendNotification(key, pre, ctx);
      notifier.notifyCacheEntryActivated(key, pre, ctx);
   }

   @ManagedAttribute(description = "number of activation events")
   public String getActivations() {
      if (!getStatisticsEnabled()) return "N/A";
      return String.valueOf(activations.get());
   }

   @ManagedOperation
   public void resetStatistics() {
      super.resetStatistics();
      activations.set(0);
   }
}


