package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationStage;

/**
 * Handles x-site data backups for pessimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class PessimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      if (skipXSiteBackup(command) || !command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, handleSingleKeyWriteReturn);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      //for pessimistic transaction we don't do a 2PC (as we already own the remote lock) but just
      //a 1PC
      throw new IllegalStateException("This should never happen!");
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
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
         keysFromMods(rCommand.getModifications().stream())
               .forEach(key -> iracManager.trackUpdatedKey(key.getSegment(), key.getKey(), rCommand.getGlobalTransaction()));
         return stage.thenReturn(rCtx, rCommand, rv);
      });
   }
}
