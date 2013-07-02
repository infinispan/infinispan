package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

/**
 * Handles x-site data backups for optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class OptimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is an "empty" tx no point replicating it to other clusters
      if (!shouldInvokeRemoteTxCommand(ctx))
         return super.visitPrepareCommand(ctx, command);

      boolean isTxFromRemoteSite = isTxFromRemoteSite(command.getGlobalTransaction());
      if (isTxFromRemoteSite) {
         return invokeNextInterceptor(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupPrepare(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!shouldInvokeRemoteTxCommand(ctx))
         return super.visitCommitCommand(ctx, command);

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNextInterceptor(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupCommit(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!shouldRollbackRemoteTxCommand(ctx))
         return super.visitRollbackCommand(ctx, command);

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNextInterceptor(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupRollback(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return result;
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
