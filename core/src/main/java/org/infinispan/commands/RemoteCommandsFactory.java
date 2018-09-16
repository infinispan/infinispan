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
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.remote.RenewBiasCommand;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.expiration.RetrieveLastAccessCommand;
import org.infinispan.commands.remote.expiration.UpdateLastAccessCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
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
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.ReplicableCommandManagerFunction;
import org.infinispan.manager.impl.ReplicableCommandRunnable;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.stream.impl.StreamIteratorCloseCommand;
import org.infinispan.stream.impl.StreamIteratorNextCommand;
import org.infinispan.stream.impl.StreamIteratorRequestCommand;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

/**
 * Specifically used to create un-initialized {@link org.infinispan.commands.ReplicableCommand}s from a byte stream.
 * This is a {@link Scopes#GLOBAL} component and doesn't have knowledge of initializing a command by injecting
 * cache-specific components into it.
 * <p>
 * Usually a second step to unmarshalling a command from a byte stream (after
 * creating an un-initialized version using this factory) is to pass the command though {@link CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)}.
 *
 * @see CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class RemoteCommandsFactory {
   @Inject private EmbeddedCacheManager cacheManager;
   @Inject @ComponentName(KnownComponentNames.MODULE_COMMAND_FACTORIES)
   private Map<Byte,ModuleCommandFactory> commandFactories;

   /**
    * Creates an un-initialized command.  Un-initialized in the sense that parameters will be set, but any components
    * specific to the cache in question will not be set.
    * <p>
    * You would typically set these parameters using {@link CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)}
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
            case CacheTopologyControlCommand.COMMAND_ID:
               command = new CacheTopologyControlCommand();
               break;
            case GetKeysInGroupCommand.COMMAND_ID:
               command = new GetKeysInGroupCommand();
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
            case ReplicableCommandRunnable.COMMAND_ID:
               command = new ReplicableCommandRunnable();
               break;
            case ReplicableCommandManagerFunction.COMMAND_ID:
               command = new ReplicableCommandManagerFunction();
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
            case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
               command = new TotalOrderNonVersionedPrepareCommand(cacheName);
               break;
            case TotalOrderVersionedPrepareCommand.COMMAND_ID:
               command = new TotalOrderVersionedPrepareCommand(cacheName);
               break;
            case CommitCommand.COMMAND_ID:
               command = new CommitCommand(cacheName);
               break;
            case VersionedCommitCommand.COMMAND_ID:
               command = new VersionedCommitCommand(cacheName);
               break;
            case TotalOrderCommitCommand.COMMAND_ID:
               command = new TotalOrderCommitCommand(cacheName);
               break;
            case TotalOrderVersionedCommitCommand.COMMAND_ID:
               command = new TotalOrderVersionedCommitCommand(cacheName);
               break;
            case RollbackCommand.COMMAND_ID:
               command = new RollbackCommand(cacheName);
               break;
            case TotalOrderRollbackCommand.COMMAND_ID:
               command = new TotalOrderRollbackCommand(cacheName);
               break;
            case SingleRpcCommand.COMMAND_ID:
               command = new SingleRpcCommand(cacheName);
               break;
            case ClusteredGetCommand.COMMAND_ID:
               command = new ClusteredGetCommand(cacheName);
               break;
            case StateRequestCommand.COMMAND_ID:
               command = new StateRequestCommand(cacheName);
               break;
            case StateResponseCommand.COMMAND_ID:
               command = new StateResponseCommand(cacheName);
               break;
            case RemoveCacheCommand.COMMAND_ID:
               command = new RemoveCacheCommand(cacheName, cacheManager);
               break;
            case TxCompletionNotificationCommand.COMMAND_ID:
               command = new TxCompletionNotificationCommand(cacheName);
               break;
            case GetInDoubtTransactionsCommand.COMMAND_ID:
               command = new GetInDoubtTransactionsCommand(cacheName);
               break;
            case DistributedExecuteCommand.COMMAND_ID:
               command = new DistributedExecuteCommand(cacheName);
               break;
            case GetInDoubtTxInfoCommand.COMMAND_ID:
               command = new GetInDoubtTxInfoCommand(cacheName);
               break;
            case CompleteTransactionCommand.COMMAND_ID:
               command = new CompleteTransactionCommand(cacheName);
               break;
            case CreateCacheCommand.COMMAND_ID:
               command = new CreateCacheCommand(cacheName);
               break;
            case XSiteAdminCommand.COMMAND_ID:
               command = new XSiteAdminCommand(cacheName);
               break;
            case CancelCommand.COMMAND_ID:
               command = new CancelCommand(cacheName);
               break;
            case XSiteStateTransferControlCommand.COMMAND_ID:
               command = new XSiteStateTransferControlCommand(cacheName);
               break;
            case XSiteStatePushCommand.COMMAND_ID:
               command = new XSiteStatePushCommand(cacheName);
               break;
            case SingleXSiteRpcCommand.COMMAND_ID:
               command = new SingleXSiteRpcCommand(cacheName);
               break;
            case ClusteredGetAllCommand.COMMAND_ID:
               command = new ClusteredGetAllCommand(cacheName);
               break;
            case StreamRequestCommand.COMMAND_ID:
               command = new StreamRequestCommand(cacheName);
               break;
            case StreamResponseCommand.COMMAND_ID:
               command = new StreamResponseCommand(cacheName);
               break;
            case StreamIteratorRequestCommand.COMMAND_ID:
               command = new StreamIteratorRequestCommand<>(cacheName);
               break;
            case StreamIteratorNextCommand.COMMAND_ID:
               command = new StreamIteratorNextCommand(cacheName);
               break;
            case StreamIteratorCloseCommand.COMMAND_ID:
               command = new StreamIteratorCloseCommand(cacheName);
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
            case InvalidateVersionsCommand.COMMAND_ID:
               command = new InvalidateVersionsCommand(cacheName);
               break;
            case RevokeBiasCommand.COMMAND_ID:
               command = new RevokeBiasCommand(cacheName);
               break;
            case RenewBiasCommand.COMMAND_ID:
               command = new RenewBiasCommand(cacheName);
               break;
            case RetrieveLastAccessCommand.COMMAND_ID:
               command = new RetrieveLastAccessCommand(cacheName);
               break;
            case UpdateLastAccessCommand.COMMAND_ID:
               command = new UpdateLastAccessCommand(cacheName);
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
