package org.infinispan.interceptors.xsite;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.irac.IracManager;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class BaseBackupInterceptor extends DDAsyncInterceptor {

   @Inject protected BackupSender backupSender;
   @Inject protected TransactionTable txTable;
   @Inject protected IracManager iracManager;
   @Inject protected ClusteringDependentLogic clusteringDependentLogic;

   protected static final Log log = LogFactory.getLog(BaseBackupInterceptor.class);
   protected static final boolean trace = log.isTraceEnabled();
   private final InvocationSuccessFunction<ClearCommand> handleClearReturn = this::handleClearReturn;
   private final InvocationSuccessAction<RemoveExpiredCommand> handleExpiredReturn = this::handleExpiredReturn;

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      if (!ctx.isOriginLocal() || skipXSiteBackup(command)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, handleClearReturn);
   }

   Object invokeNextAndWaitForCrossSite(TxInvocationContext<?> ctx, VisitableCommand command, InvocationStage stage) {
      return invokeNextThenApply(ctx, command, stage::thenReturn);
   }

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) {
      if (skipXSiteBackup(command) || !command.isMaxIdle()) {
         return invokeNext(ctx, command);
      }
      // Max idle command shouldn't fail as the timestamps are updated on access, however the remote site may have
      // a read that we aren't aware of - so we must synchronously remove the entry if expired on the remote site
      // and if it isn't expired on the remote site we must update the access time locally here
      int segment = command.getSegment();
      DistributionInfo dInfo = clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segment);
      // Only require primary to check remote site and add to irac queue - If primary dies then a backup will end up
      // doing the same as promoted primary - We also don't add tracked up to backup as we don't care if the
      // remove expired is lost due to topology change it just will cause another check later but maintain consistency
      if (dInfo.isPrimary()) {
         CompletionStage<Boolean> expired = iracManager.checkAndTrackExpiration(command.getKey());
         return asyncValue(expired).thenApply(ctx, command, (rCtx, rCommand, rv) -> {
            if ((Boolean) rv) {
               return invokeNextThenAccept(rCtx, command, this::handleExpiredReturn);
            }
            command.fail();
            return rv;
         });
      }
      return invokeNext(ctx, command);
   }

   private void handleExpiredReturn(InvocationContext context, RemoveExpiredCommand command, Object returnValue) {
      if (command.isSuccessful()) {
         iracManager.trackUpdatedKey(command.getKey(), command.getCommandInvocationId());
      }
   }

   boolean isTxFromRemoteSite(GlobalTransaction gtx) {
      LocalTransaction remoteTx = txTable.getLocalTransaction(gtx);
      return remoteTx != null && remoteTx.isFromRemoteSite();
   }

   boolean shouldInvokeRemoteTxCommand(TxInvocationContext<?> ctx) {
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

   private Object handleClearReturn(InvocationContext ctx, ClearCommand rCommand, Object rv) {
      iracManager.trackClear();
      return backupSender.backupClear(rCommand).thenReturn(ctx, rCommand, rv);
   }

   protected Stream<WriteCommand> getModificationsFrom(CommitCommand cmd) {
      GlobalTransaction gtx = cmd.getGlobalTransaction();
      LocalTransaction localTx = txTable.getLocalTransaction(gtx);
      if (localTx == null) {
         RemoteTransaction remoteTx = txTable.getRemoteTransaction(gtx);
         if (remoteTx == null) {
            if (log.isDebugEnabled()) {
               log.debugf("Transaction %s not found!", gtx);
            }
            return Stream.empty();
         } else {
            return remoteTx.getModifications().stream();
         }
      } else {
         return localTx.getModifications().stream();
      }
   }
}
