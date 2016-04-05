package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.context.InvocationContext;

import java.util.concurrent.CompletableFuture;

/**
 * Handle activation when passivation is enabled.
 *
 * @since 9.0
 */
public class ActivationInterceptor extends CacheLoaderInterceptor {

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // Load the keys for our map into the data container that we are using removing them from the store.
      // This way when we overwrite the values on commit they won't be in loader and if we rollback they won't be
      // in the loader either but will be in data container at least
      for (Object key : command.getAffectedKeys()) {
         loadIfNeeded(ctx, key, command);
      }
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   protected void sendNotification(Object key, Object value, boolean pre,
                                   InvocationContext ctx, FlagAffectedCommand cmd) {
      super.sendNotification(key, value, pre, ctx, cmd);
      notifier.notifyCacheEntryActivated(key, value, pre, ctx, cmd);
   }
}


