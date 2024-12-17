package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracMetadataRequestCommand;
import org.infinispan.commands.irac.IracPrimaryPendingKeyCheckCommand;
import org.infinispan.commands.irac.IracRequestStateCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.CheckTransactionRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.triangle.BackupNoopCommand;
import org.infinispan.commands.triangle.MultiEntriesFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.MultiKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.PutMapBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.marshall.core.Ids;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.CancelPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.NextPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.reduction.ReductionPublisherRequestCommand;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.commands.XSiteAmendOfflineStatusCommand;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteSetStateTransferModeCommand;
import org.infinispan.xsite.commands.XSiteStateTransferCancelSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferClearStatusCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferRestartSendingCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStatusRequestCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * Externalizer in charge of marshalling cache specific commands. At read time,
 * this marshaller is able to locate the right cache marshaller and provide
 * it any externalizers implementations that follow.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public final class CacheRpcCommandExternalizer extends AbstractExternalizer<CacheRpcCommand> {
   private final ReplicableCommandExternalizer cmdExt;
   private final Collection<ModuleCommandFactory> moduleCommandFactories;

   public CacheRpcCommandExternalizer(GlobalComponentRegistry gcr, ReplicableCommandExternalizer cmdExt) {
      this.moduleCommandFactories = ((Map<Byte, ModuleCommandFactory>) gcr.getComponent(KnownComponentNames.MODULE_COMMAND_FACTORIES)).values();
      this.cmdExt = cmdExt;
   }

   @Override
   public Set<Class<? extends CacheRpcCommand>> getTypeClasses() {
      Set<Class<? extends CacheRpcCommand>> collect = moduleCommandFactories.stream()
            .map(ModuleCommandFactory::getModuleCommands)
            .flatMap(m -> m.values().stream())
            .filter(CacheRpcCommand.class::isAssignableFrom)
            .map(c -> (Class<CacheRpcCommand>) c)
            .collect(Collectors.toSet());
      collect.addAll(Set.of(LockControlCommand.class,
            StateResponseCommand.class, ClusteredGetCommand.class,
            SingleRpcCommand.class, CommitCommand.class,
            PrepareCommand.class, RollbackCommand.class,
            TxCompletionNotificationCommand.class, GetInDoubtTransactionsCommand.class,
            GetInDoubtTxInfoCommand.class, CompleteTransactionCommand.class,
            VersionedPrepareCommand.class,
            VersionedCommitCommand.class,
            XSiteStatePushCommand.class,
            ClusteredGetAllCommand.class,
            SingleKeyBackupWriteCommand.class,
            SingleKeyFunctionalBackupWriteCommand.class,
            PutMapBackupWriteCommand.class,
            MultiEntriesFunctionalBackupWriteCommand.class,
            MultiKeyFunctionalBackupWriteCommand.class,
            SizeCommand.class,
            BackupNoopCommand.class,
            ReductionPublisherRequestCommand.class,
            MultiClusterEventCommand.class, InitialPublisherCommand.class, NextPublisherCommand.class,
            CancelPublisherCommand.class, CheckTransactionRpcCommand.class,
            XSiteAmendOfflineStatusCommand.class, XSiteStateTransferCancelSendCommand.class,
            XSiteStateTransferClearStatusCommand.class, XSiteStateTransferFinishReceiveCommand.class,
            XSiteStateTransferFinishSendCommand.class, XSiteStateTransferRestartSendingCommand.class,
            XSiteStateTransferStartReceiveCommand.class, XSiteStateTransferStartSendCommand.class,
            XSiteStateTransferStatusRequestCommand.class, ConflictResolutionStartCommand.class,
            StateTransferCancelCommand.class, StateTransferGetListenersCommand.class,
            StateTransferGetTransactionsCommand.class, StateTransferStartCommand.class,
            IracCleanupKeysCommand.class, IracMetadataRequestCommand.class,
            IracRequestStateCommand.class, IracStateResponseCommand.class,
            IracUpdateVersionCommand.class,
            XSiteAutoTransferStatusCommand.class,
            XSiteSetStateTransferModeCommand.class,
            IracTombstoneCleanupCommand.class,
            IracTombstoneStateResponseCommand.class,
            IracTombstonePrimaryCheckCommand.class,
            IracTombstoneRemoteSiteCheckCommand.class,
            IracPrimaryPendingKeyCheckCommand.class));
      return collect;
   }

   @Override
   public void writeObject(ObjectOutput output, CacheRpcCommand command) throws IOException {
      //header: type + method id.
      cmdExt.writeCommandHeader(output, command);
      ByteString cacheName = command.getCacheName();
      ByteString.writeObject(output, cacheName);

      // Take the cache marshaller and generate the payload for the rest of
      // the command using that cache marshaller and the write the bytes in
      // the original payload.
      marshallParameters(command, output);
   }

   private void marshallParameters(CacheRpcCommand cmd, ObjectOutput oo) throws IOException {
      cmdExt.writeCommandParameters(oo, cmd);
   }

   @Override
   public CacheRpcCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      //header
      byte type = input.readByte();
      byte methodId = (byte) input.readShort();
      ByteString cacheName = ByteString.readObject(input);

      //create the object input
      CacheRpcCommand cacheRpcCommand = cmdExt.fromStream(methodId, type, cacheName);
      cmdExt.readCommandParameters(input, cacheRpcCommand);
      return cacheRpcCommand;
   }

   @Override
   public Integer getId() {
      return Ids.CACHE_RPC_COMMAND;
   }

}
