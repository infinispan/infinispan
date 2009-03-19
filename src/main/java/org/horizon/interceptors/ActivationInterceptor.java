package org.horizon.interceptors;

import org.horizon.commands.read.GetKeyValueCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.config.ConfigurationException;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Start;
import org.horizon.jmx.annotations.ManagedAttribute;
import org.horizon.jmx.annotations.ManagedOperation;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheStore;

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
      for (Object k : keys) store.remove(k);
   }

   @Override
   protected void sendNotification(Object key, boolean pre, InvocationContext ctx) {
      super.sendNotification(key, pre, ctx);
      notifier.notifyCacheEntryActivated(key, pre, ctx);
   }

   @ManagedAttribute(description = "number of activation events")
   public long getActivations() {
      return activations.get();
   }

   @ManagedOperation
   public void resetStatistics() {
      super.resetStatistics();
      activations.set(0);
   }
}


