package org.infinispan.commands;

import java.util.Map;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
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
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.iteration.impl.EntryRequestCommand;
import org.infinispan.iteration.impl.EntryResponseCommand;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.stream.impl.StreamSegmentResponseCommand;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

/**
 * Specifically used to create un-initialized {@link org.infinispan.commands.ReplicableCommand}s from a byte stream.
 * This is a {@link Scopes#GLOBAL} component and doesn't have knowledge of initializing a command by injecting
 * cache-specific components into it.
 * <p />
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
   EmbeddedCacheManager cacheManager;
   GlobalComponentRegistry registry;
   Map<Byte,ModuleCommandFactory> commandFactories;

   @Inject
   public void inject(EmbeddedCacheManager cacheManager, GlobalComponentRegistry registry,
                      @ComponentName(KnownComponentNames.MODULE_COMMAND_FACTORIES) Map<Byte, ModuleCommandFactory> commandFactories) {
      this.cacheManager = cacheManager;
      this.registry = registry;
      this.commandFactories = commandFactories;
   }

   /**
    * Creates an un-initialized command.  Un-initialized in the sense that parameters will be set, but any components
    * specific to the cache in question will not be set.
    * <p/>
    * You would typically set these parameters using {@link CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)}
    * <p/>
    *
    *
    * @param id id of the command
    * @param parameters parameters to set
    * @param type
    * @return a replicable command
    */
   public ReplicableCommand fromStream(byte id, Object[] parameters, byte type) {
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
            case ApplyDeltaCommand.COMMAND_ID:
               command = new ApplyDeltaCommand();
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
            default:
               throw new CacheException("Unknown command id " + id + "!");
         }
      } else {
         ModuleCommandFactory mcf = commandFactories.get(id);
         if (mcf != null)
            return mcf.fromStream(id, parameters);
         else
            throw new CacheException("Unknown command id " + id + "!");
      }
      command.setParameters(id, parameters);
      return command;
   }

   /**
    * Resolve an {@link CacheRpcCommand} from the stream.
    *
    * @param id            id of the command
    * @param parameters    parameters to be set
    * @param type          type of command (whether internal or user defined)
    * @param cacheName     cache name at which this command is directed
    * @return              an instance of {@link CacheRpcCommand}
    */
   public CacheRpcCommand fromStream(byte id, Object[] parameters, byte type, String cacheName) {
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
            case MultipleRpcCommand.COMMAND_ID:
               command = new MultipleRpcCommand(cacheName);
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
               ComponentRegistry namedCacheRegistry = registry.getNamedComponentRegistry(cacheName);
               command = new RemoveCacheCommand(cacheName, cacheManager, this.registry,
                     namedCacheRegistry.getComponent(PersistenceManager.class),
                     namedCacheRegistry.getComponent(CacheJmxRegistration.class));
               break;
            case TxCompletionNotificationCommand.COMMAND_ID:
               command = new TxCompletionNotificationCommand(cacheName);
               break;
            case GetInDoubtTransactionsCommand.COMMAND_ID:
               command = new GetInDoubtTransactionsCommand(cacheName);
               break;
            case MapCombineCommand.COMMAND_ID:
               command = new MapCombineCommand(cacheName);
               break;
            case ReduceCommand.COMMAND_ID:
               command = new ReduceCommand(cacheName);
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
            case EntryRequestCommand.COMMAND_ID:
               command = new EntryRequestCommand(cacheName);
               break;
            case EntryResponseCommand.COMMAND_ID:
               command = new EntryResponseCommand(cacheName);
               break;
            case ClusteredGetAllCommand.COMMAND_ID:
               command = new ClusteredGetAllCommand(cacheName);
               break;
            case StreamRequestCommand.COMMAND_ID:
               command = new StreamRequestCommand(cacheName);
               break;
            case StreamSegmentResponseCommand.COMMAND_ID:
               command = new StreamSegmentResponseCommand<>(cacheName);
               break;
            case StreamResponseCommand.COMMAND_ID:
               command = new StreamResponseCommand(cacheName);
               break;
            default:
               throw new CacheException("Unknown command id " + id + "!");
         }
      } else {
         ExtendedModuleCommandFactory mcf = (ExtendedModuleCommandFactory) commandFactories.get(id);
         if (mcf != null)
            return mcf.fromStream(id, parameters, cacheName);
         else
            throw new CacheException("Unknown command id " + id + "!");
      }
      command.setParameters(id, parameters);
      return command;
   }
}
