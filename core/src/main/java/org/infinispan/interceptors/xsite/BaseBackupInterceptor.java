package org.infinispan.interceptors.xsite;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;

import java.util.concurrent.CompletableFuture;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BaseBackupInterceptor extends DDSequentialInterceptor {

   protected BackupSender backupSender;
   protected TransactionTable txTable;

   private static final Log log = LogFactory.getLog(BaseBackupInterceptor.class);

   @Inject
   void init(BackupSender sender, TransactionTable txTable) {
      this.backupSender = sender;
      this.txTable = txTable;
   }

   @Override
   public final CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

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
      return processBackupResponse(ctx, command, backupResponse);
   }

   protected CompletableFuture<Void> processBackupResponse(TxInvocationContext ctx, VisitableCommand command,
         BackupResponse backupResponse) {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null)
            throw throwable;

         backupSender.processResponses(backupResponse, command, ctx.getTransaction());
         return null;
      });
   }

   protected CompletableFuture<Void> handleMultipleKeysWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || skipXSiteBackup(command)) {
         return ctx.continueInvocation();
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            backupSender.processResponses(backupSender.backupWrite(command), command);
         }
         return null;
      });
   }

   protected boolean isTxFromRemoteSite(GlobalTransaction gtx) {
      LocalTransaction remoteTx = txTable.getLocalTransaction(gtx);
      return remoteTx != null && remoteTx.isFromRemoteSite();
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      // ISPN-2362: For backups, we should only replicate to the remote site if there are modifications to replay.
      boolean shouldBackupRemotely = ctx.isOriginLocal() && ctx.hasModifications() &&
            !ctx.getCacheTransaction().isFromStateTransfer();
      getLog().tracef("Should backup remotely? %s", shouldBackupRemotely);
      return shouldBackupRemotely;
   }

   protected final boolean skipXSiteBackup(FlagAffectedCommand command) {
      return command.hasFlag(Flag.SKIP_XSITE_BACKUP);
   }

   protected Log getLog() {
      return log;
   }
}
