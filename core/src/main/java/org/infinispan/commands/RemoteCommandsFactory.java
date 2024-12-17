package org.infinispan.commands;

import java.util.Map;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracMetadataRequestCommand;
import org.infinispan.commands.irac.IracRequestStateCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
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
import org.infinispan.commands.topology.CacheAvailabilityUpdateCommand;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.commands.topology.CacheLeaveCommand;
import org.infinispan.commands.topology.CacheShutdownCommand;
import org.infinispan.commands.topology.CacheShutdownRequestCommand;
import org.infinispan.commands.topology.CacheStatusRequestCommand;
import org.infinispan.commands.topology.RebalancePhaseConfirmCommand;
import org.infinispan.commands.topology.RebalancePolicyUpdateCommand;
import org.infinispan.commands.topology.RebalanceStartCommand;
import org.infinispan.commands.topology.RebalanceStatusRequestCommand;
import org.infinispan.commands.topology.TopologyUpdateCommand;
import org.infinispan.commands.topology.TopologyUpdateStableCommand;
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
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.ReplicableManagerFunctionCommand;
import org.infinispan.manager.impl.ReplicableRunnableCommand;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.CancelPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.NextPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.reduction.ReductionPublisherRequestCommand;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.commands.XSiteAmendOfflineStatusCommand;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteLocalEventCommand;
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
 * Specifically used to create un-initialized {@link org.infinispan.commands.ReplicableCommand}s from a byte stream.
 * This is a {@link Scopes#GLOBAL} component and doesn't have knowledge of initializing a command by injecting
 * cache-specific components into it.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class RemoteCommandsFactory {
   @Inject EmbeddedCacheManager cacheManager;
   @Inject GlobalComponentRegistry globalComponentRegistry;
   @Inject @ComponentName(KnownComponentNames.MODULE_COMMAND_FACTORIES)
   Map<Byte,ModuleCommandFactory> commandFactories;

   /**
    * Creates an un-initialized command.  Un-initialized in the sense that parameters will be set, but any components
    * specific to the cache in question will not be set.
    *
    * @param id id of the command
    * @param type type of the command
    * @return a replicable command
    */
   public ReplicableCommand fromStream(byte id, byte type) {
      ReplicableCommand command;
      if (type == 0) {
         switch (id) {
            case PutKeyValueCommand.COMMAND_ID:
               command = new PutKeyValueCommand();
               break;
            case PutMapCommand.COMMAND_ID:
               command = new PutMapCommand();
               break;
            case RemoveCommand.COMMAND_ID:
               command = new RemoveCommand();
               break;
            case ReplaceCommand.COMMAND_ID:
               command = new ReplaceCommand();
               break;
            case ComputeCommand.COMMAND_ID:
               command = new ComputeCommand();
               break;
            case ComputeIfAbsentCommand.COMMAND_ID:
               command = new ComputeIfAbsentCommand();
               break;
            case GetKeyValueCommand.COMMAND_ID:
               command = new GetKeyValueCommand();
               break;
            case ClearCommand.COMMAND_ID:
               command = new ClearCommand();
               break;
            case InvalidateCommand.COMMAND_ID:
               command = new InvalidateCommand();
               break;
            case InvalidateL1Command.COMMAND_ID:
               command = new InvalidateL1Command();
               break;
            case GetCacheEntryCommand.COMMAND_ID:
               command = new GetCacheEntryCommand();
               break;
            case ReadWriteKeyCommand.COMMAND_ID:
               command = new ReadWriteKeyCommand<>();
               break;
            case ReadWriteKeyValueCommand.COMMAND_ID:
               command = new ReadWriteKeyValueCommand<>();
               break;
            case ReadWriteManyCommand.COMMAND_ID:
               command = new ReadWriteManyCommand<>();
               break;
            case ReadWriteManyEntriesCommand.COMMAND_ID:
               command = new ReadWriteManyEntriesCommand<>();
               break;
            case WriteOnlyKeyCommand.COMMAND_ID:
               command = new WriteOnlyKeyCommand<>();
               break;
            case WriteOnlyKeyValueCommand.COMMAND_ID:
               command = new WriteOnlyKeyValueCommand<>();
               break;
            case WriteOnlyManyCommand.COMMAND_ID:
               command = new WriteOnlyManyCommand<>();
               break;
            case WriteOnlyManyEntriesCommand.COMMAND_ID:
               command = new WriteOnlyManyEntriesCommand<>();
               break;
            case RemoveExpiredCommand.COMMAND_ID:
               command = new RemoveExpiredCommand();
               break;
            case ReplicableRunnableCommand.COMMAND_ID:
               command = new ReplicableRunnableCommand();
               break;
            case ReplicableManagerFunctionCommand.COMMAND_ID:
               command = new ReplicableManagerFunctionCommand();
               break;
            case ReadOnlyKeyCommand.COMMAND_ID:
               command = new ReadOnlyKeyCommand();
               break;
            case ReadOnlyManyCommand.COMMAND_ID:
               command = new ReadOnlyManyCommand<>();
               break;
            case TxReadOnlyKeyCommand.COMMAND_ID:
               command = new TxReadOnlyKeyCommand<>();
               break;
            case TxReadOnlyManyCommand.COMMAND_ID:
               command = new TxReadOnlyManyCommand<>();
               break;
            case HeartBeatCommand.COMMAND_ID:
               command = HeartBeatCommand.INSTANCE;
               break;
            case CacheJoinCommand.COMMAND_ID:
               command = new CacheJoinCommand();
               break;
            case CacheLeaveCommand.COMMAND_ID:
               command = new CacheLeaveCommand();
               break;
            case RebalancePhaseConfirmCommand.COMMAND_ID:
               command = new RebalancePhaseConfirmCommand();
               break;
            case RebalancePolicyUpdateCommand.COMMAND_ID:
               command = new RebalancePolicyUpdateCommand();
               break;
            case RebalanceStartCommand.COMMAND_ID:
               command = new RebalanceStartCommand();
               break;
            case RebalanceStatusRequestCommand.COMMAND_ID:
               command = new RebalanceStatusRequestCommand();
               break;
            case CacheShutdownRequestCommand.COMMAND_ID:
               command = new CacheShutdownRequestCommand();
               break;
            case CacheShutdownCommand.COMMAND_ID:
               command = new CacheShutdownCommand();
               break;
            case TopologyUpdateCommand.COMMAND_ID:
               command = new TopologyUpdateCommand();
               break;
            case CacheStatusRequestCommand.COMMAND_ID:
               command = new CacheStatusRequestCommand();
               break;
            case TopologyUpdateStableCommand.COMMAND_ID:
               command = new TopologyUpdateStableCommand();
               break;
            case CacheAvailabilityUpdateCommand.COMMAND_ID:
               command = new CacheAvailabilityUpdateCommand();
               break;
            case IracPutKeyValueCommand.COMMAND_ID:
               command = new IracPutKeyValueCommand();
               break;
            case TouchCommand.COMMAND_ID:
               command = new TouchCommand();
               break;
            case XSiteLocalEventCommand.COMMAND_ID:
               command = new XSiteLocalEventCommand();
               break;
            default:
               throw new CacheException("Unknown command id " + id + "!");
         }
      } else {
         ModuleCommandFactory mcf = commandFactories.get(id);
         if (mcf != null)
            return mcf.fromStream(id);
         else
            throw new CacheException("Unknown command id " + id + "!");
      }
      return command;
   }

   /**
    * Resolve an {@link CacheRpcCommand} from the stream.
    *
    * @param id            id of the command
    * @param type          type of command (whether internal or user defined)
    * @param cacheName     cache name at which this command is directed
    * @return              an instance of {@link CacheRpcCommand}
    */
   public CacheRpcCommand fromStream(byte id, byte type, ByteString cacheName) {
      CacheRpcCommand command;
      if (type == 0) {
         switch (id) {
            case LockControlCommand.COMMAND_ID:
               command = new LockControlCommand(cacheName);
               break;
            case PrepareCommand.COMMAND_ID:
               command = new PrepareCommand(cacheName);
               break;
            case VersionedPrepareCommand.COMMAND_ID:
               command = new VersionedPrepareCommand(cacheName);
               break;
            case CommitCommand.COMMAND_ID:
               command = new CommitCommand(cacheName);
               break;
            case VersionedCommitCommand.COMMAND_ID:
               command = new VersionedCommitCommand(cacheName);
               break;
            case RollbackCommand.COMMAND_ID:
               command = new RollbackCommand(cacheName);
               break;
            case SingleRpcCommand.COMMAND_ID:
               command = new SingleRpcCommand(cacheName);
               break;
            case ClusteredGetCommand.COMMAND_ID:
               command = new ClusteredGetCommand(cacheName);
               break;
            case ConflictResolutionStartCommand.COMMAND_ID:
               command = new ConflictResolutionStartCommand(cacheName);
               break;
            case StateTransferCancelCommand.COMMAND_ID:
               command = new StateTransferCancelCommand(cacheName);
               break;
            case StateTransferStartCommand.COMMAND_ID:
               command = new StateTransferStartCommand(cacheName);
               break;
            case StateTransferGetListenersCommand.COMMAND_ID:
               command = new StateTransferGetListenersCommand(cacheName);
               break;
            case StateTransferGetTransactionsCommand.COMMAND_ID:
               command = new StateTransferGetTransactionsCommand(cacheName);
               break;
            case StateResponseCommand.COMMAND_ID:
               command = new StateResponseCommand(cacheName);
               break;
            case TxCompletionNotificationCommand.COMMAND_ID:
               command = new TxCompletionNotificationCommand(cacheName);
               break;
            case GetInDoubtTransactionsCommand.COMMAND_ID:
               command = new GetInDoubtTransactionsCommand(cacheName);
               break;
            case GetInDoubtTxInfoCommand.COMMAND_ID:
               command = new GetInDoubtTxInfoCommand(cacheName);
               break;
            case CompleteTransactionCommand.COMMAND_ID:
               command = new CompleteTransactionCommand(cacheName);
               break;
            case XSiteAmendOfflineStatusCommand.COMMAND_ID:
               command = new XSiteAmendOfflineStatusCommand(cacheName);
               break;
            case XSiteStateTransferCancelSendCommand.COMMAND_ID:
               command = new XSiteStateTransferCancelSendCommand(cacheName);
               break;
            case XSiteStateTransferClearStatusCommand.COMMAND_ID:
               command = new XSiteStateTransferClearStatusCommand(cacheName);
               break;
            case XSiteStateTransferFinishReceiveCommand.COMMAND_ID:
               command = new XSiteStateTransferFinishReceiveCommand(cacheName);
               break;
            case XSiteStateTransferFinishSendCommand.COMMAND_ID:
               command = new XSiteStateTransferFinishSendCommand(cacheName);
               break;
            case XSiteStateTransferRestartSendingCommand.COMMAND_ID:
               command = new XSiteStateTransferRestartSendingCommand(cacheName);
               break;
            case XSiteStateTransferStartReceiveCommand.COMMAND_ID:
               command = new XSiteStateTransferStartReceiveCommand(cacheName);
               break;
            case XSiteStateTransferStartSendCommand.COMMAND_ID:
               command = new XSiteStateTransferStartSendCommand(cacheName);
               break;
            case XSiteStateTransferStatusRequestCommand.COMMAND_ID:
               command = new XSiteStateTransferStatusRequestCommand(cacheName);
               break;
            case XSiteStatePushCommand.COMMAND_ID:
               command = new XSiteStatePushCommand(cacheName);
               break;
            case ClusteredGetAllCommand.COMMAND_ID:
               command = new ClusteredGetAllCommand(cacheName);
               break;
            case SingleKeyBackupWriteCommand.COMMAND_ID:
               command = new SingleKeyBackupWriteCommand(cacheName);
               break;
            case SingleKeyFunctionalBackupWriteCommand.COMMAND_ID:
               command = new SingleKeyFunctionalBackupWriteCommand(cacheName);
               break;
            case PutMapBackupWriteCommand.COMMAND_ID:
               command = new PutMapBackupWriteCommand(cacheName);
               break;
            case MultiEntriesFunctionalBackupWriteCommand.COMMAND_ID:
               command = new MultiEntriesFunctionalBackupWriteCommand(cacheName);
               break;
            case MultiKeyFunctionalBackupWriteCommand.COMMAND_ID:
               command = new MultiKeyFunctionalBackupWriteCommand(cacheName);
               break;
            case BackupNoopCommand.COMMAND_ID:
               command = new BackupNoopCommand(cacheName);
               break;
            case ReductionPublisherRequestCommand.COMMAND_ID:
               command = new ReductionPublisherRequestCommand<>(cacheName);
               break;
            case MultiClusterEventCommand.COMMAND_ID:
               command = new MultiClusterEventCommand<>(cacheName);
               break;
            case InitialPublisherCommand.COMMAND_ID:
               command = new InitialPublisherCommand<>(cacheName);
               break;
            case NextPublisherCommand.COMMAND_ID:
               command = new NextPublisherCommand(cacheName);
               break;
            case CancelPublisherCommand.COMMAND_ID:
               command = new CancelPublisherCommand(cacheName);
               break;
            case CheckTransactionRpcCommand.COMMAND_ID:
               command = new CheckTransactionRpcCommand(cacheName);
               break;
            case IracCleanupKeysCommand.COMMAND_ID:
               command = new IracCleanupKeysCommand(cacheName);
               break;
            case IracMetadataRequestCommand.COMMAND_ID:
               command = new IracMetadataRequestCommand(cacheName);
               break;
            case IracRequestStateCommand.COMMAND_ID:
               command = new IracRequestStateCommand(cacheName);
               break;
            case IracStateResponseCommand.COMMAND_ID:
               command = new IracStateResponseCommand(cacheName);
               break;
            case IracUpdateVersionCommand.COMMAND_ID:
               command = new IracUpdateVersionCommand(cacheName);
               break;
            case XSiteAutoTransferStatusCommand.COMMAND_ID:
               command = new XSiteAutoTransferStatusCommand(cacheName);
               break;
            case XSiteSetStateTransferModeCommand.COMMAND_ID:
               command = new XSiteSetStateTransferModeCommand(cacheName);
               break;
            case IracTombstoneCleanupCommand.COMMAND_ID:
               command = new IracTombstoneCleanupCommand(cacheName);
               break;
            case IracTombstoneRemoteSiteCheckCommand.COMMAND_ID:
               command = new IracTombstoneRemoteSiteCheckCommand(cacheName);
               break;
            case IracTombstoneStateResponseCommand.COMMAND_ID:
               command = new IracTombstoneStateResponseCommand(cacheName);
               break;
            case IracTombstonePrimaryCheckCommand.COMMAND_ID:
               command = new IracTombstonePrimaryCheckCommand(cacheName);
               break;
            case SizeCommand.COMMAND_ID:
               command = new SizeCommand(cacheName);
               break;
            default:
               throw new CacheException("Unknown command id " + id + "!");
         }
      } else {
         ModuleCommandFactory mcf = commandFactories.get(id);
         if (mcf != null)
            return mcf.fromStream(id, cacheName);
         else
            throw new CacheException("Unknown command id " + id + "!");
      }
      return command;
   }
}
