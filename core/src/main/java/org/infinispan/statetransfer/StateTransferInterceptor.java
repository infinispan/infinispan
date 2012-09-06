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

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

   private long rpcTimeout;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration, RpcManager rpcManager, StateTransferManager stateTransferManager) {
      this.stateTransferLock = stateTransferLock;
      this.rpcManager = rpcManager;
      this.stateTransferManager = stateTransferManager;
      // no need to retry for asynchronous caches
      this.rpcTimeout = configuration.clustering().cacheMode().isSynchronous()
            ? configuration.clustering().sync().replTimeout() : 0;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
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
   private Object handleTxCommand(InvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      return handleTopologyAffectedCommand(ctx, command);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      return handleTopologyAffectedCommand(ctx, command);
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, (TopologyAffectedCommand) command);
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   private Object handleTopologyAffectedCommand(InvocationContext ctx, TopologyAffectedCommand command) throws Throwable {
      if (ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final boolean isTxCommand = command instanceof TransactionBoundaryCommand;
         try {
            if (isTxCommand) {
               stateTransferLock.transactionsSharedLock();
            }
            return invokeNextInterceptor(ctx, command);
         } finally {
            if (isTxCommand) {
               stateTransferLock.transactionsSharedUnlock();
            }
         }
      }

      Set<Address> newTargets = null;
      stateTransferLock.commandsSharedLock();
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      final int topologyId = cacheTopology.getTopologyId();
      final ConsistentHash readCh = cacheTopology.getReadConsistentHash();
      final ConsistentHash writeCh = cacheTopology.getWriteConsistentHash();

      // set the topology id if it was not set before (ie. this is not a remote or forwarded command)
      if (command.getTopologyId() == -1) {
         command.setTopologyId(cacheTopology.getTopologyId());
      }
      try {
         final boolean isTxCommand = command instanceof TransactionBoundaryCommand;
         if (isTxCommand) {
            stateTransferLock.transactionsSharedLock();
         }
         try {
            // forward commands with older topology ids to their new targets
            if (command.getTopologyId() < topologyId) {
               // if it is a read request and comes from an older topology we need to check if we still hold the data
               Object readKey = null;
               if (command instanceof GetKeyValueCommand) {   //todo [anistor] would be nice to have a common ReadCommand interface for these
                  readKey = ((GetKeyValueCommand) command).getKey();
               } else if (command instanceof ClusteredGetCommand) {
                  readKey = ((ClusteredGetCommand) command).getKey();
               }
               if (readKey != null) {
                  // it's a read operation
                  if (!readCh.isKeyLocalToNode(rpcManager.getAddress(), readKey)) {
                     return null; //todo [anistor] throw an exception or return a special result that will cause the read command to be retried on the originator
                  }
               } else if (command instanceof PrepareCommand || command instanceof LockControlCommand || command instanceof WriteCommand) {  //todo a ClearCommand should be executed directly
                  // a TX or a write command from an old topology should be forwarded unless it's a write and the context is transactional
                  if (command instanceof WriteCommand && ctx instanceof TxInvocationContext) {
                     // a transactional write is always local
                     return invokeNextInterceptor(ctx, command);
                  } else {
                     Set<Object> affectedKeys = getAffectedKeys(ctx, command);
                     newTargets = new HashSet<Address>();
                     boolean localExecutionNeeded = false;
                     for (Object key : affectedKeys) {
                        if (writeCh.isKeyLocalToNode(rpcManager.getAddress(), key)) {
                           localExecutionNeeded = true;
                        } else {
                           newTargets.addAll(writeCh.locateOwners(key));
                        }
                     }

                     if (localExecutionNeeded) {
                        return invokeNextInterceptor(ctx, command);
                     }
                  }
               } else if (command instanceof CommitCommand || command instanceof RollbackCommand) {
                  // for these commands we can determine affected keys only after they are executed
                  try {
                     // it does not harm to attempt to execute them if it might not be the proper destination
                     return invokeNextInterceptor(ctx, command);
                  } finally {
                     newTargets = new HashSet<Address>();
                     Set<Object> affectedKeys = ((TxInvocationContext) ctx).getAffectedKeys();
                     for (Object key : affectedKeys) {
                        if (!writeCh.isKeyLocalToNode(rpcManager.getAddress(), key)) {
                           newTargets.addAll(writeCh.locateOwners(key));
                        }
                     }
                  }
               }
            } else if (command.getTopologyId() > topologyId) {
               // this means there will be a new topology installed soon
               stateTransferLock.waitForTopology(command.getTopologyId());

               // proceed normally
            } else {
               // proceed normally
            }

            // no special handling was needed, invoke normally (and do not forward)
            return invokeNextInterceptor(ctx, command);
         } finally {
            if (isTxCommand) {
               stateTransferLock.transactionsSharedUnlock();
            }
         }
      } finally {
         stateTransferLock.commandsSharedUnlock();

         log.tracef("Forwarding command %s to new targets %", command, newTargets);
         if (newTargets != null && !newTargets.isEmpty()) {
            rpcManager.invokeRemotely(newTargets, command, true);
         }
      }
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