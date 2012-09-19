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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Takes care of replicating modifications to other caches in a cluster.
 *
 * @author Bela Ban
 * @since 4.0
 */
public class ReplicationInterceptor extends BaseRpcInterceptor {

   protected CommandsFactory cf;

   private StateTransferManager stateTransferManager;

   private boolean isPessimisticCache;

   private static final Log log = LogFactory.getLog(ReplicationInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(CommandsFactory cf, StateTransferManager stateTransferManager) {
      this.cf = cf;
      this.stateTransferManager = stateTransferManager;
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
      // may need to resend, so make the commit command synchronous
      // TODO keep the list of prepared nodes or the view id when the prepare command was sent to know whether we need to resend the prepare info
      rpcManager.invokeRemotely(null, command, cacheConfiguration.transaction().syncCommitPhase(), true);
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
      boolean async = cacheConfiguration.clustering().cacheMode() == CacheMode.REPL_ASYNC;
      rpcManager.broadcastRpcCommand(command, !async, false);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx) && !Configurations.isOnePhaseCommit(cacheConfiguration)) {
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
               returnValue = remoteGet(ctx, command, false);
            }
         }
         return returnValue;
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }

   private boolean needsRemoteGet(InvocationContext ctx, AbstractDataCommand command) {
      Object key = command.getKey();
      final CacheEntry entry;
      return !command.hasFlag(Flag.CACHE_MODE_LOCAL)
            && !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)   //todo [anistor] do we need this? it should normally be used only in distributed mode, never in replicated mode
            && !stateTransferManager.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key)
            && ((entry = ctx.lookupEntry(key)) == null || entry.isNull() || entry.isLockPlaceholder());
   }

   //todo [anistor] need to revise these methods
   /**
    * This method retrieves an entry from a remote cache.
    * <p/>
    * This method only works if a) this is a locally originating invocation and b) the entry in question is not local to
    * the current cache instance and c) the entry is not in L1.  If either of a, b or c does not hold true, this method
    * returns a null and doesn't do anything.
    *
    *
    * @param ctx invocation context
    * @param command
    * @return value of a remote get, or null
    * @throws Throwable if there are problems
    */
   private Object remoteGet(InvocationContext ctx, AbstractDataCommand command, boolean isWrite) throws Throwable {
      Object key = command.getKey();
      if (trace) {
         log.tracef("Key %s is not yet available on %s, so we may need to look elsewhere", key, rpcManager.getAddress());
      }
      boolean acquireRemoteLock = false;
      if (ctx.isInTxScope()) {
         TxInvocationContext txContext = (TxInvocationContext) ctx;
         acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
      }
      // attempt a remote lookup
      InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command.getFlags());

      if (acquireRemoteLock) {
         ((TxInvocationContext) ctx).addAffectedKey(key);
      }

      return ice != null ? ice.getValue() : null;
   }

   private InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, Set<Flag> flags) {
      GlobalTransaction gtx = acquireRemoteLock ? ((TxInvocationContext)ctx).getGlobalTransaction() : null;
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, flags, acquireRemoteLock, gtx);

      List<Address> targets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));
      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      targets.retainAll(rpcManager.getTransport().getMembers());
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

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleCrudMethod(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleCrudMethod(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleCrudMethod(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleCrudMethod(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleCrudMethod(ctx, command);
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleCrudMethod(final InvocationContext ctx, final WriteCommand command) throws Throwable {
      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to replicate.
      final Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isLocalModeForced(command) && command.isSuccessful() && ctx.isOriginLocal() && !ctx.isInTxScope()) {
         if (ctx.isUseFutureReturnType()) {
            NotifyingNotifiableFuture<Object> future = new NotifyingFutureImpl(returnValue);
            rpcManager.broadcastRpcCommandInFuture(command, future);
            return future;
         } else {
            rpcManager.broadcastRpcCommand(command, isSynchronous(command));
         }
      }
      return returnValue;
   }

}
