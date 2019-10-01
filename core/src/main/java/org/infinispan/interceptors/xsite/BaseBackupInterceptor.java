package org.infinispan.interceptors.xsite;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
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

   @Inject protected BackupSender backupSender;
   @Inject protected TransactionTable txTable;

   protected static final Log log = LogFactory.getLog(BaseBackupInterceptor.class);
   protected static final boolean trace = log.isTraceEnabled();
   private final InvocationSuccessFunction<ClearCommand> handleClearReturn = this::handleClearReturn;

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      if (!ctx.isOriginLocal() || skipXSiteBackup(command)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, handleClearReturn);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if this is an "empty" tx no point replicating it to other clusters
      if (!shouldInvokeRemoteTxCommand(ctx)) return invokeNext(ctx, command);

      boolean isTxFromRemoteSite = isTxFromRemoteSite(command.getGlobalTransaction());
      if (isTxFromRemoteSite) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage = backupSender.backupPrepare(command, ctx.getCacheTransaction(), ctx.getTransaction());
      return invokeNextAndWaitForCrossSite(ctx, command, stage);
   }

   Object invokeNextAndWaitForCrossSite(TxInvocationContext ctx, VisitableCommand command, InvocationStage stage) {
      return invokeNextThenApply(ctx, command, stage::thenReturn);
   }

   boolean isTxFromRemoteSite(GlobalTransaction gtx) {
      LocalTransaction remoteTx = txTable.getLocalTransaction(gtx);
      return remoteTx != null && remoteTx.isFromRemoteSite();
   }

   boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      // ISPN-2362: For backups, we should only replicate to the remote site if there are modifications to replay.
      boolean shouldBackupRemotely =
            ctx.isOriginLocal() && ctx.hasModifications() && !ctx.getCacheTransaction().isFromStateTransfer();
      getLog().tracef("Should backup remotely? %s", shouldBackupRemotely);
      return shouldBackupRemotely;
   }

   static boolean skipXSiteBackup(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP);
   }

   protected Log getLog() {
      return log;
   }

   private Object handleClearReturn(InvocationContext ctx, ClearCommand rCommand, Object rv) throws Throwable {
      InvocationStage stage = backupSender.backupWrite(rCommand, rCommand);
      return stage.thenReturn(ctx, rCommand, rv);
   }
}
