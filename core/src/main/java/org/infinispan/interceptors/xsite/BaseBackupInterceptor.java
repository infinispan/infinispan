package org.infinispan.interceptors.xsite;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BaseBackupInterceptor extends DDAsyncInterceptor {

   protected BackupSender backupSender;
   protected TransactionTable txTable;

   private static final Log log = LogFactory.getLog(BaseBackupInterceptor.class);

   @Inject
   void init(BackupSender sender, TransactionTable txTable) {
      this.backupSender = sender;
      this.txTable = txTable;
   }

   @Override
   public final BasicInvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is an "empty" tx no point replicating it to other clusters
      if (!shouldInvokeRemoteTxCommand(ctx)) return invokeNext(ctx, command);

      boolean isTxFromRemoteSite = isTxFromRemoteSite(command.getGlobalTransaction());
      if (isTxFromRemoteSite) {
         return invokeNext(ctx, command);
      }

      BackupResponse backupResponse = backupSender.backupPrepare(command);
      return processBackupResponse(ctx, command, backupResponse);
   }

   protected BasicInvocationStage processBackupResponse(TxInvocationContext ctx, VisitableCommand command,
         BackupResponse backupResponse) {
      return invokeNext(ctx, command).thenAccept(
            (rCtx, rCommand, rv) -> backupSender.processResponses(backupResponse, command, ctx.getTransaction()));
   }

   protected BasicInvocationStage handleMultipleKeysWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (!ctx.isOriginLocal() || skipXSiteBackup(command)) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command).thenAccept(
            (rCtx, rCommand, rv) -> backupSender.processResponses(backupSender.backupWrite(command), command));
   }

   protected boolean isTxFromRemoteSite(GlobalTransaction gtx) {
      LocalTransaction remoteTx = txTable.getLocalTransaction(gtx);
      return remoteTx != null && remoteTx.isFromRemoteSite();
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      // ISPN-2362: For backups, we should only replicate to the remote site if there are modifications to replay.
      boolean shouldBackupRemotely =
            ctx.isOriginLocal() && ctx.hasModifications() && !ctx.getCacheTransaction().isFromStateTransfer();
      getLog().tracef("Should backup remotely? %s", shouldBackupRemotely);
      return shouldBackupRemotely;
   }

   protected final boolean skipXSiteBackup(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP);
   }

   protected Log getLog() {
      return log;
   }
}
