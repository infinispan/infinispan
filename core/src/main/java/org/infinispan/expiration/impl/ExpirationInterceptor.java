package org.infinispan.expiration.impl;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.Set;

/**
 * Interceptor used to ensure that when a writie occurs we don't expire the entry at the same time.  This can be both
 * costly by going remote an extra time or even removing from the cache store when not required as it will be
 * overwritten.
 */
public class ExpirationInterceptor<K, V> extends CommandInterceptor {
   private ExpirationManager<K, V> expirationManager;

   @Inject
   public void inject(ExpirationManager<K, V> expirationManager) {
      this.expirationManager = expirationManager;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // TODO: do we want to stop all expirations?
      return super.visitClearCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Set<K> keys = (Set<K>) command.getAffectedKeys();
      keys.forEach(expirationManager::registerWriteIncoming);
      try {
         return super.visitPutMapCommand(ctx, command);
      } finally {
         // TODO: what about for transactions?
         keys.forEach(expirationManager::unregisterWrite);
      }
   }

   private Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      K key = (K) command.getKey();
      expirationManager.registerWriteIncoming(key);
      try {
         return handleDefault(ctx, command);
      } finally {
         // TODO: what about for transactions?
         expirationManager.unregisterWrite(key);
      }
   }
}
