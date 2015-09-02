package org.infinispan.expiration.impl;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

import java.util.Set;

/**
 * Interceptor to be used with optimistic transactions to make sure prepare doesn't cause unneeded expirations
 */
public class OptimisticTxExpirationInterceptor<K, V> extends ExpirationInterceptor<K, V> {
   private final RegisterInterceptor registerInterceptor = new RegisterInterceptor();
   private final UnregisterInterceptor unregisterInterceptor = new UnregisterInterceptor();

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      // We only need to keep register for prepare as commit command doesn't read the entry
      for (WriteCommand cmd : command.getModifications()) {
         cmd.acceptVisitor(ctx, registerInterceptor);
      }
      try {
         return super.visitPrepareCommand(ctx, command);
      } finally {
         for (WriteCommand cmd : command.getModifications()) {
            cmd.acceptVisitor(ctx, unregisterInterceptor);
         }
      }
   }
   
   private abstract class ExpirationVisitor extends AbstractVisitor {
      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return handleSingleKey((K) command.getKey());
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return handleSingleKey((K) command.getKey());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         ((Set<K>) command.getAffectedKeys()).forEach(this::handleSingleKey);
         return null;
      }

      protected abstract Void handleSingleKey(K key);
   }

   private class RegisterInterceptor extends ExpirationVisitor {
      protected Void handleSingleKey(K key) {
         expirationManager.registerWriteIncoming(key);
         return null;
      }
   }

   private class UnregisterInterceptor extends ExpirationVisitor {
      protected Void handleSingleKey(K key) {
         expirationManager.unregisterWrite(key);
         return null;
      }
   }
}
