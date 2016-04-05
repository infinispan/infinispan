package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Handles x-site data backups for pessimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class PessimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is an "empty" tx no point replicating it to other clusters
      if (!shouldInvokeRemoteTxCommand(ctx))
         return ctx.continueInvocation();

      boolean isTxFromRemoteSite = isTxFromRemoteSite( command.getGlobalTransaction() );
      if (isTxFromRemoteSite) {
         return ctx.continueInvocation();
      }

      BackupResponse backupResponse = backupSender.backupPrepare(command);
      Object result = ctx.forkInvocationSync(command);
      backupSender.processResponses(backupResponse, command, ctx.getTransaction());
      return ctx.shortCircuit(result);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      //for pessimistic transaction we don't do a 2PC (as we already own the remote lock) but just
      //a 1PC
      throw new IllegalStateException("This should never happen!");
   }
}
