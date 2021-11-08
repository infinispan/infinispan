package org.infinispan.interceptors.xsite;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.SegmentAwareKey;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.ch.KeyPartitioner;
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
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected CommandsFactory commandsFactory;

   private static final Log log = LogFactory.getLog(BaseBackupInterceptor.class);
   private final InvocationSuccessFunction<ClearCommand> handleClearReturn = this::handleClearReturn;
   private final InvocationSuccessFunction<RemoveExpiredCommand> handleBackupMaxIdle = this::handleBackupMaxIdle;
   private final InvocationSuccessAction<RemoveExpiredCommand> handleExpiredReturn = this::handleExpiredReturn;
   protected final InvocationSuccessFunction<DataWriteCommand> handleSingleKeyWriteReturn = this::handleSingleKeyWriteReturn;

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      if (skipXSiteBackup(command)) {
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
         return asyncValue(expired).thenApply(ctx, command, handleBackupMaxIdle);
      }
      return invokeNext(ctx, command);
   }

   private Object handleBackupMaxIdle(InvocationContext rCtx, RemoveExpiredCommand rCommand, Object rv) {
      if ((Boolean) rv) {
         return invokeNextThenAccept(rCtx, rCommand, handleExpiredReturn);
      }
      rCommand.fail();
      return rv;
   }

   private void handleExpiredReturn(InvocationContext context, RemoveExpiredCommand command, Object returnValue) {
      if (command.isSuccessful()) {
         iracManager.trackExpiredKey(command.getSegment(), command.getKey(), command.getCommandInvocationId());
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
      log.tracef("Should backup remotely? %s", shouldBackupRemotely);
      return shouldBackupRemotely;
   }

   static boolean skipXSiteBackup(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP);
   }

   private static boolean backupToRemoteSite(WriteCommand command) {
      return !command.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP);
   }

   private Stream<SegmentAwareKey<?>> keyStream(WriteCommand command) {
      return command.getAffectedKeys().stream().map(key -> SegmentSpecificCommand.extractSegmentAwareKey(command, key, keyPartitioner));
   }

   private Object handleClearReturn(InvocationContext ctx, ClearCommand rCommand, Object rv) {
      iracManager.trackClear(ctx.isOriginLocal());
      return ctx.isOriginLocal() ? backupSender.backupClear(rCommand).thenReturn(ctx, rCommand, rv) : rv;
   }

   private Object handleSingleKeyWriteReturn(InvocationContext ctx, DataWriteCommand dataWriteCommand, Object rv) {
      if (!dataWriteCommand.isSuccessful()) {
         if (log.isTraceEnabled()) {
            log.tracef("Command %s is not successful, not replicating", dataWriteCommand);
         }
         return rv;
      }
      int segment = dataWriteCommand.getSegment();
      if (clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segment).isPrimary()) {
         //primary owner always tracks updates to the remote sites (and sends the update in the background)
         iracManager.trackUpdatedKey(segment, dataWriteCommand.getKey(), dataWriteCommand.getCommandInvocationId());
         CacheEntry<?,?> entry = ctx.lookupEntry(dataWriteCommand.getKey());
         WriteCommand crossSiteCommand = createCommandForXSite(entry, segment, dataWriteCommand.getFlagsBitSet());
         return backupSender.backupWrite(crossSiteCommand, dataWriteCommand).thenReturn(ctx, dataWriteCommand, rv);
      } else if (!ctx.isOriginLocal()) {
         // backup owners need to keep track of the update in the remote context for ASYNC cross-site
         // if backup owner == originator, we don't want to track the key again when ctx.isOriginLocal==true
         iracManager.trackUpdatedKey(segment, dataWriteCommand.getKey(), dataWriteCommand.getCommandInvocationId());
      }
      return rv;
   }

   private WriteCommand createCommandForXSite(CacheEntry<?, ?> entry, int segment, long flagsBitSet) {
      return entry.isRemoved() ?
            commandsFactory.buildRemoveCommand(entry.getKey(), null, segment, flagsBitSet) :
            commandsFactory.buildPutKeyValueCommand(entry.getKey(), entry.getValue(), segment, entry.getMetadata(),
                  flagsBitSet);
   }

   private boolean isWriteOwner(SegmentAwareKey<?> key) {
      return clusteringDependentLogic.getCacheTopology().isSegmentWriteOwner(key.getSegment());
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

   public Stream<SegmentAwareKey<?>> keysFromMods(Stream<WriteCommand> modifications) {
      return modifications
            .filter(WriteCommand::isSuccessful)
            .filter(BaseBackupInterceptor::backupToRemoteSite)
            .flatMap(this::keyStream)
            .filter(this::isWriteOwner);
   }
}
