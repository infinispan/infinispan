package org.infinispan.interceptors.locking;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class NonTransactionalLockingInterceptor extends AbstractLockingInterceptor {

   private static final Log log = LogFactory.getLog(NonTransactionalLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected final Object visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);//possibly needed because of L1 locks being acquired
      }
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);//possibly needed because of L1 locks being acquired
      }
   }

   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         if (!command.isForwarded() && !hasSkipLocking(command)) {
            lockAllAndRecord(ctx, command.getMap().keySet().stream().filter(this::shouldLockKey), getLockTimeoutMillis(command));
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   private void assertNonTransactional(InvocationContext ctx) {
      //this only happens if the cache is used in a transaction's scope
      if (ctx.isInTxScope()) {
         throw new InvalidCacheUsageException(
               "This is a non-transactional cache and cannot be accessed with a transactional InvocationContext.");
      }
   }
}
