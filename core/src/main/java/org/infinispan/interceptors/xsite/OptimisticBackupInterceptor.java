package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Handles x-site data backups for optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class OptimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!shouldInvokeRemoteTxCommand(ctx))
         return ctx.continueInvocation();

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return ctx.continueInvocation();
      }

      BackupResponse backupResponse = backupSender.backupCommit(command);
      return processBackupResponse(ctx, command, backupResponse);
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!shouldRollbackRemoteTxCommand(ctx))
         return ctx.continueInvocation();

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return ctx.continueInvocation();
      }

      BackupResponse backupResponse = backupSender.backupRollback(command);
      return processBackupResponse(ctx, command, backupResponse);
   }

   private boolean shouldRollbackRemoteTxCommand(TxInvocationContext ctx) {
      return shouldInvokeRemoteTxCommand(ctx) && hasBeenPrepared((LocalTxInvocationContext) ctx);
   }

   /**
    * This 'has been prepared' logic only applies to optimistic transactions, hence it is not present in the
    * LocalTransaction object itself.
    */
   private boolean hasBeenPrepared(LocalTxInvocationContext ctx) {
      return !ctx.getRemoteLocksAcquired().isEmpty();
   }
}
