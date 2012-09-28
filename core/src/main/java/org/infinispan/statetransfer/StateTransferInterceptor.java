/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.*;
import org.infinispan.commands.write.*;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.WriteSkewHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

//todo [anistor] command forwarding breaks the rule that we have only one originator for a command. this opens now the possibility to have two threads processing incoming remote commands for the same TX
/**
 * // TODO: Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferInterceptor extends CommandInterceptor {   //todo [anistor] this interceptor should be added to stack only if we have state transfer. maybe we need this for invalidation mode too!

   private static final Log log = LogFactory.getLog(StateTransferInterceptor.class);

   private final AffectedKeysVisitor affectedKeysVisitor = new AffectedKeysVisitor();

   private StateTransferLock stateTransferLock;

   private StateTransferManager stateTransferManager;

   private RpcManager rpcManager;

   private CommandsFactory commandFactory;

   private long rpcTimeout;

   private boolean useVersioning;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration, RpcManager rpcManager,
                    CommandsFactory commandFactory, StateTransferManager stateTransferManager) {
      this.stateTransferLock = stateTransferLock;
      this.rpcManager = rpcManager;
      this.commandFactory = commandFactory;
      this.stateTransferManager = stateTransferManager;

      // no need to retry for asynchronous caches
      rpcTimeout = configuration.clustering().cacheMode().isSynchronous()
            ? configuration.clustering().sync().replTimeout() : 0;

      useVersioning = configuration.transaction().transactionMode().isTransactional() && configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC && configuration.versioning().enabled();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.getCacheTransaction() instanceof RemoteTransaction) {
         ((RemoteTransaction) ctx.getCacheTransaction()).setMissingLookedUpEntries(false);
      }

      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (ctx.getCacheTransaction() instanceof RemoteTransaction) {
         // If a commit is received for a transaction that doesn't have its 'lookedUpEntries' populated
         // we know for sure this transaction is 2PC and was received via state transfer but the preceding PrepareCommand
         // was not received by local node because it was executed on the previous key owners. We need to re-prepare
         // the transaction on local node to ensure its locks are acquired and lookedUpEntries is properly populated.
         RemoteTransaction remoteTx = (RemoteTransaction) ctx.getCacheTransaction();
         if (remoteTx.isMissingLookedUpEntries()) {
            remoteTx.setMissingLookedUpEntries(false);

            PrepareCommand prepareCommand;
            if (useVersioning) {
               prepareCommand = commandFactory.buildVersionedPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
               WriteSkewHelper.setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) prepareCommand, ctx);
            } else {
               prepareCommand = commandFactory.buildPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
            }
            commandFactory.initializeReplicableCommand(prepareCommand, true);
            prepareCommand.setOrigin(ctx.getOrigin());
            prepareCommand.perform(null);
         }
      }

      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      // there is not state transfer in invalidation mode so there is not need to forward this command to new owners
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      // no need to forward this command
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // it's not necessary to propagate eviction to the new owners in case of state transfer
      return invokeNextInterceptor(ctx, command);
   }

   /**
    * Special processing required for transaction commands.
    *
    * @param ctx
    * @param command
    * @return
    * @throws Throwable
    */
   private Object handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command)
         throws Throwable {
      return handleTopologyAffectedCommand(ctx, command, ctx.getCacheTransaction() instanceof RemoteTransaction);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      return handleTopologyAffectedCommand(ctx, command, ctx.isOriginLocal());
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, (TopologyAffectedCommand) command, ctx.isOriginLocal());
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   private Object handleTopologyAffectedCommand(InvocationContext ctx, TopologyAffectedCommand command,
                                                boolean originLocal) throws Throwable {
      boolean cacheModeLocal = false;
      if (command instanceof FlagAffectedCommand) {
         cacheModeLocal = ((FlagAffectedCommand)command).hasFlag(Flag.CACHE_MODE_LOCAL);
      }
      if (originLocal || cacheModeLocal) {
         return invokeNextInterceptor(ctx, command);
      }

      // set the topology id if it was not set before (ie. this is local command)
      // TODO Make tx commands extend FlagAffectedCommand so we can use CACHE_MODE_LOCAL in StaleTransactionCleanupService
      if (command.getTopologyId() == -1) {
         command.setTopologyId(stateTransferManager.getCacheTopology().getTopologyId());
      }

      // remote/forwarded command
      int cmdTopologyId = command.getTopologyId();
      stateTransferLock.waitForTransactionData(cmdTopologyId);

      // TODO we may need to skip local invocation for read/write/tx commands if the command is too old and none of its keys are local
      Object localResult = invokeNextInterceptor(ctx, command);

      // forward commands with older topology ids to their new targets
      // but we need to make sure we have the latest topology
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      int localTopologyId = cacheTopology.getTopologyId();
      // if it's a tx/lock/write command, forward it to the new owners
      if (cmdTopologyId < localTopologyId) {
         if (command instanceof TransactionBoundaryCommand || command instanceof LockControlCommand
               || (command instanceof WriteCommand && !ctx.isInTxScope())) {
            // We don't know the full topology history to send the command only to the new owners,
            // but we do know two things:
            // 1. The originator - which shouldn't receive the same command again
            // 2. If the local topology = command topology + 1 and pendingCH = null, there are no new owners
            ConsistentHash pendingCh = cacheTopology.getPendingCH();
            if (pendingCh != null && cmdTopologyId < localTopologyId + 1) {
               ConsistentHash writeCh = cacheTopology.getWriteConsistentHash();
               Set<Object> affectedKeys = getAffectedKeys(ctx, command);
               Set<Address> newTargets = writeCh.locateAllOwners(affectedKeys);
               newTargets.remove(rpcManager.getAddress());
               if (!newTargets.isEmpty()) {
                  // Update the topology id to prevent cycles
                  command.setTopologyId(localTopologyId);
                  log.tracef("Forwarding command %s to new targets %s", command, newTargets);
                  // TODO find a way to forward the command async if it was received async
                  rpcManager.invokeRemotely(newTargets, command, true, false);
               }
            }
         }
      }

      return localResult;
   }

   @SuppressWarnings("unchecked")
   private Set<Object> getAffectedKeys(InvocationContext ctx, VisitableCommand command) {
      Set<Object> affectedKeys = null;
      try {
         affectedKeys = (Set<Object>) command.acceptVisitor(ctx, affectedKeysVisitor);
      } catch (Throwable throwable) {
         // impossible to reach this
      }
      if (affectedKeys == null) {
         affectedKeys = Collections.emptySet();
      }
      return affectedKeys;
   }
}