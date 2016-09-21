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
package org.infinispan.interceptors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Takes care of replicating modifications to other caches in a cluster.
 *
 * @author Bela Ban
 * @since 4.0
 */
public class ReplicationInterceptor extends ClusteringInterceptor {

   private boolean isPessimisticCache;

   private static final Log log = LogFactory.getLog(ReplicationInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Start
   public void start() {
      isPessimisticCache = cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isInTxScope()) throw new IllegalStateException("This should not be possible!");
      if (shouldInvokeRemoteTxCommand(ctx)) {
         sendCommitCommand(command);
      }
      return invokeNextInterceptor(ctx, command);
   }

   private void sendCommitCommand(CommitCommand command)
         throws TimeoutException, InterruptedException {
      boolean syncCommitPhase = cacheConfiguration.transaction().syncCommitPhase();
      ResponseMode responseMode =
            syncCommitPhase ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING;
      long replTimeout = cacheConfiguration.clustering().sync().replTimeout();
      Map<Address, Response> responseMap = rpcManager.invokeRemotely(null, command, responseMode, replTimeout);
      checkTxCommandResponses(responseMap, command);
   }

   protected void checkTxCommandResponses(Map<Address, Response> responseMap, TransactionBoundaryCommand command) {
      for (Map.Entry<Address, Response> e : responseMap.entrySet()) {
         Address recipient = e.getKey();
         Response response = e.getValue();
         if (response instanceof CacheNotFoundResponse) {
            // No need to retry if the missing node wasn't a member when the command started.
            if (command.getTopologyId() == stateTransferManager.getCacheTopology().getTopologyId()
                  && !rpcManager.getMembers().contains(recipient)) {
               log.tracef("Ignoring response from node not targeted %s", recipient);
            } else {
               log.tracef("Cache not running on node %s, or the node is missing", recipient);
               throw new OutdatedTopologyException("Cache not running on node " + recipient);
            }
         } else if (response instanceof UnsureResponse) {
            log.tracef("Node %s has a newer topology id", recipient);
            throw new OutdatedTopologyException("Cache not running on node " + recipient);
         }
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         broadcastPrepare(ctx, command);
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(rpcManager.getTransport().getMembers());
      }
      return retVal;
   }

   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         boolean sync = cacheConfiguration.clustering().cacheMode().isSynchronous();
         if (sync) {
            ResponseMode responseMode = ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS;
            long replTimeout = cacheConfiguration.clustering().sync().replTimeout();
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(null, command, responseMode, replTimeout);
            checkTxCommandResponses(responseMap, command);
         } else {
            // The invokeRemotely overload that takes a ResponseMode parameter doesn't use the replication queue
            rpcManager.broadcastRpcCommand(command, false);
         }
      } finally {
         transactionRemotelyPrepared(context);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      boolean prepareSent = ctx.isOriginLocal() && ((LocalTransaction) ctx.getCacheTransaction()).isPrepareSent();
      //If we are using onePhaseCommit, the PrepareCommand cleanups everything related to the transaction (release
      //locks, remove the transaction from transaction table, etc...). So we don't need to waste resource to send the
      //command that is ignored in the other nodes.
      if (shouldInvokeRemoteTxCommand(ctx) && !(prepareSent && Configurations.isOnePhaseCommit(cacheConfiguration))) {
         rpcManager.broadcastRpcCommand(command, cacheConfiguration.transaction().syncRollbackPhase(), true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         Object returnValue = invokeNextInterceptor(ctx, command);

         // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
         // available.  It could just have been removed in the same tx beforehand.  Also don't bother with a remote get if
         // the entry is mapped to the local node.
         if (returnValue == null && ctx.isOriginLocal()) {
            if (needsRemoteGet(ctx, command)) {
               returnValue = remoteGet(ctx, command.getKey(), command, false);
            }
            if (returnValue == null) {
               returnValue = localGet(ctx, command.getKey(), false, command);
            }
         }
         return returnValue;
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }
   
   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (ctx.isOriginLocal()) {
         //unlock will happen async as it is a best effort
         boolean sync = !command.isUnlock();
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(rpcManager.getTransport().getMembers());
         if (sync) {
            ResponseMode responseMode = ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS;
            long replTimeout = cacheConfiguration.clustering().sync().replTimeout();
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(null, command, responseMode, replTimeout);
            checkTxCommandResponses(responseMap, command);
         } else {
            rpcManager.broadcastRpcCommand(command, false, false);
         }
      }
      return retVal;
   }

   /**
    * This method retrieves an entry from a remote cache.
    * <p/>
    * This method only works if a) this is a locally originating invocation and b) the entry in question is not local to
    * the current cache instance and c) the entry is not in L1.  If either of a, b or c does not hold true, this method
    * returns a null and doesn't do anything.
    *
    *
    * @param ctx invocation context
    * @param key
    * @param command
    * @return value of a remote get, or null
    * @throws Throwable if there are problems
    */
   private Object remoteGet(InvocationContext ctx, Object key, FlagAffectedCommand command, boolean isWrite) throws Throwable {
      if (trace) {
         log.tracef("Key %s is not yet available on %s, so we may need to look elsewhere", key, rpcManager.getAddress());
      }
      boolean acquireRemoteLock = false;
      if (ctx.isInTxScope()) {
         TxInvocationContext txContext = (TxInvocationContext) ctx;
         acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
      }
      // attempt a remote lookup
      InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command);

      if (acquireRemoteLock) {
         ((TxInvocationContext) ctx).addAffectedKey(key);
      }

      if (ice != null) {
         if (!ctx.replaceValue(key, ice.getValue()))  {
            if (isWrite) {
               lockAndWrap(ctx, key, ice, command);
            } else {
               ctx.putLookedUpEntry(key, ice);
            }
         }
         return ice.getValue();
      }
      return null;
   }

   protected Address getPrimaryOwner() {
      return stateTransferManager.getCacheTopology().getReadConsistentHash().getMembers().get(0);
   }

   protected InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command) {
      GlobalTransaction gtx = acquireRemoteLock ? ((TxInvocationContext)ctx).getGlobalTransaction() : null;
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, command.getFlags(), acquireRemoteLock, gtx);

      List<Address> targets = Collections.singletonList(getPrimaryOwner());
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, rpcManager.getAddress());
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, ResponseMode.WAIT_FOR_VALID_RESPONSE,
            cacheConfiguration.clustering().sync().replTimeout(), true, filter);

      if (!responses.isEmpty()) {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
               return cacheValue.toInternalCacheEntry(key);
            }
         }
      }

      return null;
   }

   private Object localGet(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      InternalCacheEntry ice = dataContainer.get(key);
      if (ice != null) {
         if (!ctx.replaceValue(key, ice.getValue())) {
            if (isWrite)
               lockAndWrap(ctx, key, ice, command);
            else
               ctx.putLookedUpEntry(key, ice);
         }
         return command instanceof GetCacheEntryCommand ? ice : ice.getValue();
      }
      return null;
   }

   private void lockAndWrap(InvocationContext ctx, Object key, InternalCacheEntry ice, FlagAffectedCommand command) throws InterruptedException {
      if (isPessimisticCache && rpcManager.getAddress().equals(getPrimaryOwner())) {
         boolean skipLocking = hasSkipLocking(command);
         long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
         lockManager.acquireLock(ctx, key, lockTimeout, skipLocking);
      }
      entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleCrudMethod(ctx, command, !ctx.isOriginLocal());
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleCrudMethod(ctx, command, true);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return handleCrudMethod(ctx, command, !ctx.isOriginLocal());
      } finally {
         if (ignorePreviousValueOnBackup(command, ctx)) {
            // the command that will execute remotely must ignore previous values
            command.setIgnorePreviousValue(true);
         }
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleCrudMethod(ctx, command, true);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return handleCrudMethod(ctx, command, !ctx.isOriginLocal());
      } finally {
         if (ignorePreviousValueOnBackup(command, ctx)) {
            // the command that will execute remotely must ignore previous values
            command.setIgnorePreviousValue(true);
         }
      }
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleCrudMethod(InvocationContext ctx, WriteCommand command, boolean skipRemoteGet) throws Throwable {
      if (!skipRemoteGet || command.hasFlag(Flag.DELTA_WRITE)) {
         remoteGetBeforeWrite(ctx, command);
      }

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to replicate.
      final Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isLocalModeForced(command) && command.isSuccessful() && ctx.isOriginLocal() && !ctx.isInTxScope()) {
         rpcManager.broadcastRpcCommand(command, isSynchronous(command));
      }
      return returnValue;
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (command instanceof AbstractDataCommand && (isNeedReliableReturnValues(command) || command.isConditional()) || command.hasFlag(Flag.DELTA_WRITE)) {
         AbstractDataCommand singleKeyCommand = (AbstractDataCommand) command;

         Object returnValue = null;
         // get it remotely if we do not have it yet
         if (needsRemoteGet(ctx, singleKeyCommand)) {
            returnValue = remoteGet(ctx, singleKeyCommand.getKey(), singleKeyCommand, true);
         }
         if (returnValue == null) {
            localGet(ctx, singleKeyCommand.getKey(), true, command);
         }
      }
   }
}
