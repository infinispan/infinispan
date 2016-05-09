package org.infinispan.interceptors.locking;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Locking interceptor to be used for non-transactional caches.
 *
 * @author Mircea Markus
 */
public class NonTransactionalLockingInterceptor extends AbstractLockingInterceptor {

   private static final Log log = LogFactory.getLog(NonTransactionalLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected final CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      assertNonTransactional(ctx);
      // TODO Check if the return handler is really needed
      //possibly needed because of L1 locks being acquired
      return ctx.onReturn(unlockAllReturnHandler);
   }

   @Override
   protected CompletableFuture<Void> visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      assertNonTransactional(ctx);
      // TODO Check if the return handler is really needed
      //possibly needed because of L1 locks being acquired
      return ctx.onReturn(unlockAllReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      assertNonTransactional(ctx);
      if (command.isForwarded() || hasSkipLocking(command)) {
         return ctx.continueInvocation();
      }
      ctx.onReturn(unlockAllReturnHandler);
      lockAllAndRecord(ctx, command.getMap().keySet().stream().filter(this::shouldLockKey), getLockTimeoutMillis(command));
      return ctx.continueInvocation();
   }

   private void assertNonTransactional(InvocationContext ctx) {
      //this only happens if the cache is used in a transaction's scope
      if (ctx.isInTxScope()) {
         throw new InvalidCacheUsageException(
               "This is a non-transactional cache and cannot be accessed with a transactional InvocationContext.");
      }
   }
}
