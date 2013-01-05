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
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.WriteSkewHelper;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

//todo [anistor] command forwarding breaks the rule that we have only one originator for a command. this opens now the possibility to have two threads processing incoming remote commands for the same TX
/**
 * // TODO: Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferInterceptor extends CommandInterceptor {   //todo [anistor] this interceptor should be added to stack only if we have state transfer. maybe we need this for invalidation mode too!

   private static final Log log = LogFactory.getLog(StateTransferInterceptor.class);

   private StateTransferLock stateTransferLock;

   private StateTransferManager stateTransferManager;

   private CommandsFactory commandFactory;

   private boolean useVersioning;

   private final AffectedKeysVisitor affectedKeysVisitor = new AffectedKeysVisitor();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration,
                    CommandsFactory commandFactory, StateTransferManager stateTransferManager) {
      this.stateTransferLock = stateTransferLock;
      this.commandFactory = commandFactory;
      this.stateTransferManager = stateTransferManager;

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
            log.tracef("Replaying the transactions received as a result of state transfer %s", prepareCommand);
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
    */
   private Object handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      // For local commands we may not have a GlobalTransaction yet
      Address address = ctx.isOriginLocal() ? ctx.getOrigin() : ctx.getGlobalTransaction().getAddress();
      return handleTopologyAffectedCommand(ctx, command, address, true);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      // ISPN-2473 - forward non-transactional commands asynchronously
      return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin(), false);
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin(), true);
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   private Object handleTopologyAffectedCommand(InvocationContext ctx, VisitableCommand command,
                                                Address origin, boolean sync) throws Throwable {
      log.tracef("handleTopologyAffectedCommand for command %s", command);

      if (isLocalOnly(ctx, command)) {
         return invokeNextInterceptor(ctx, command);
      }
      updateTopologyIdAndWaitForTransactionData((TopologyAffectedCommand) command);

      // TODO we may need to skip local invocation for read/write/tx commands if the command is too old and none of its keys are local
      Object localResult = invokeNextInterceptor(ctx, command);

      boolean isNonTransactionalWrite = !ctx.isInTxScope() && command instanceof WriteCommand;
      boolean isTransactionalAndNotRolledBack = false;
      if (ctx.isInTxScope()) {
         isTransactionalAndNotRolledBack = !((TxInvocationContext)ctx).getCacheTransaction().isMarkedForRollback();
      }

      if (isNonTransactionalWrite || isTransactionalAndNotRolledBack) {
         stateTransferManager.forwardCommandIfNeeded(((TopologyAffectedCommand)command), getAffectedKeys(ctx, command), origin, sync);
      }

      return localResult;
   }

   private void updateTopologyIdAndWaitForTransactionData(TopologyAffectedCommand command) throws InterruptedException {
      // set the topology id if it was not set before (ie. this is local command)
      // TODO Make tx commands extend FlagAffectedCommand so we can use CACHE_MODE_LOCAL in StaleTransactionCleanupService
      if (command.getTopologyId() == -1) {
         CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         if (cacheTopology != null) {
            command.setTopologyId(cacheTopology.getTopologyId());
         }
      }

      // remote/forwarded command
      int cmdTopologyId = command.getTopologyId();
      stateTransferLock.waitForTransactionData(cmdTopologyId);
   }

   private boolean isLocalOnly(InvocationContext ctx, VisitableCommand command) {
      boolean transactionalWrite = ctx.isInTxScope() && command instanceof WriteCommand;
      boolean cacheModeLocal = false;
      if (command instanceof FlagAffectedCommand) {
         cacheModeLocal = ((FlagAffectedCommand)command).hasFlag(Flag.CACHE_MODE_LOCAL);
      }
      return cacheModeLocal || transactionalWrite;
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
         affectedKeys = InfinispanCollections.emptySet();
      }
      return affectedKeys;
   }
}