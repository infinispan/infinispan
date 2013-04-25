/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.commands;

import org.infinispan.CacheException;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
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
import org.infinispan.commands.write.*;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.xsite.XSiteAdminCommand;

import java.util.Map;

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
            case VersionedPutKeyValueCommand.COMMAND_ID:
               command = new VersionedPutKeyValueCommand();
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
            case VersionedReplaceCommand.COMMAND_ID:
               command = new VersionedReplaceCommand();
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
               command = new RemoveCacheCommand(cacheName, cacheManager, registry,
                     registry.getNamedComponentRegistry(cacheName).getComponent(CacheLoaderManager.class));
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
