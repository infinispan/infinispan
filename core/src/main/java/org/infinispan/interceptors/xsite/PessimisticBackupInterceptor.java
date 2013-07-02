package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

/**
 * Handles x-site data backups for pessimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class PessimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is an "empty" tx no point replicating it to other clusters
      if (!shouldInvokeRemoteTxCommand(ctx))
         return super.visitPrepareCommand(ctx, command);

      boolean isTxFromRemoteSite = isTxFromRemoteSite( command.getGlobalTransaction() );
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
      //for pessimistic transaction we don't do a 2PC (as we already own the remote lock) but just
      //a 1PC
      throw new IllegalStateException("This should never happen!");
   }
}
