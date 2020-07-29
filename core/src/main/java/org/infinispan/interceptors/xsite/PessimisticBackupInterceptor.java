package org.infinispan.interceptors.xsite;

import java.util.Arrays;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.interceptors.InvocationStage;

/**
 * Handles x-site data backups for pessimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class PessimisticBackupInterceptor extends BaseBackupInterceptor {

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      //for pessimistic transaction we don't do a 2PC (as we already own the remote lock) but just
      //a 1PC
      throw new IllegalStateException("This should never happen!");
   }

   @Override
   protected Object visitBackupRemoveExpired(DistributionInfo info, InvocationContext ctx, RemoveExpiredCommand command) {
      return checkRemoteSiteIfMaxIdleExpired(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage;
      if (shouldInvokeRemoteTxCommand(ctx)) {
         //for sync, the originator is the one who backups the transaction to the remote site.
         stage = backupSender.backupPrepare(command, ctx.getCacheTransaction(), ctx.getTransaction());
      } else {
         stage = InvocationStage.completedNullStage();
      }

      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         //for async, all nodes need to keep track of the updates keys after it is applied locally.
         iracManager.trackKeysFromTransaction(Arrays.stream(rCommand.getModifications()), rCommand.getGlobalTransaction());
         return stage.thenReturn(rCtx, rCommand, rv);
      });
   }
}
