/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.interceptors.base;

import org.horizon.commands.CacheRPCCommand;
import org.horizon.commands.CommandsFactory;
import org.horizon.commands.ReplicableCommand;
import org.horizon.commands.remote.ReplicateCommand;
import org.horizon.context.InvocationContext;
import org.horizon.context.TransactionContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.invocation.Flag;
import org.horizon.remoting.RPCManager;
import org.horizon.remoting.ReplicationQueue;
import org.horizon.remoting.ResponseMode;
import org.horizon.remoting.transport.Address;
import org.horizon.transaction.GlobalTransaction;
import org.horizon.transaction.TransactionTable;

import javax.transaction.Transaction;
import java.util.List;

/**
 * Acts as a base for all RPC calls - subclassed by
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public abstract class BaseRpcInterceptor extends CommandInterceptor {
   private ReplicationQueue replicationQueue;
   protected TransactionTable txTable;
   private CommandsFactory commandsFactory;

   protected RPCManager rpcManager;
   protected boolean defaultSynchronous;
   private boolean stateTransferEnabled;

   @Inject
   public void injectComponents(RPCManager rpcManager, ReplicationQueue replicationQueue,
                                TransactionTable txTable, CommandsFactory commandsFactory) {
      this.rpcManager = rpcManager;
      this.replicationQueue = replicationQueue;
      this.txTable = txTable;
      this.commandsFactory = commandsFactory;
   }

   @Start
   public void init() {
      defaultSynchronous = configuration.getCacheMode().isSynchronous();
      stateTransferEnabled = configuration.isFetchInMemoryState();
   }

   /**
    * Checks whether any of the responses are exceptions. If yes, re-throws them (as exceptions or runtime exceptions).
    */
   protected void checkResponses(List rsps) throws Throwable {
      if (rsps != null) {
         for (Object rsp : rsps) {
            if (rsp != null && rsp instanceof Throwable) {
               // lets print a stack trace first.
               if (log.isDebugEnabled())
                  log.debug("Received Throwable from remote cache", (Throwable) rsp);
               throw (Throwable) rsp;
            }
         }
      }
   }

   protected void replicateCall(InvocationContext ctx, CacheRPCCommand call, boolean sync, boolean useOutOfBandMessage) throws Throwable {
      replicateCall(ctx, null, call, sync, useOutOfBandMessage);
   }

   protected void replicateCall(InvocationContext ctx, ReplicableCommand call, boolean sync, boolean useOutOfBandMessage) throws Throwable {
      replicateCall(ctx, null, call, sync, useOutOfBandMessage);
   }

   protected void replicateCall(InvocationContext ctx, CacheRPCCommand call, boolean sync) throws Throwable {
      replicateCall(ctx, null, call, sync, false);
   }

   protected void replicateCall(InvocationContext ctx, ReplicableCommand call, boolean sync) throws Throwable {
      replicateCall(ctx, null, call, sync, false);
   }

   protected void replicateCall(InvocationContext ctx, List<Address> recipients, ReplicableCommand c, boolean sync, boolean useOutOfBandMessage) throws Throwable {
      long syncReplTimeout = configuration.getSyncReplTimeout();

      if (ctx.hasFlag(Flag.FORCE_ASYNCHRONOUS)) sync = false;
      else if (ctx.hasFlag(Flag.FORCE_SYNCHRONOUS)) sync = true;

      // tx-level overrides are more important
      Transaction tx = ctx.getTransaction();
      if (tx != null) {
         TransactionContext transactionContext = ctx.getTransactionContext();
         if (transactionContext != null) {
            if (transactionContext.isForceAsyncReplication()) sync = false;
            else if (transactionContext.isForceSyncReplication()) sync = true;
         }
      }

      replicateCall(recipients, c, sync, useOutOfBandMessage, syncReplTimeout);
   }

   protected void replicateCall(List<Address> recipients, ReplicableCommand call, boolean sync, boolean useOutOfBandMessage, long timeout) throws Throwable {
      if (trace) log.trace("Broadcasting call " + call + " to recipient list " + recipients);

      if (!sync && replicationQueue != null) {
         if (log.isDebugEnabled()) log.debug("Putting call " + call + " on the replication queue.");
         replicationQueue.add(call);
      } else {
         List<Address> callRecipients = recipients;
         if (callRecipients == null) {
            callRecipients = null;
            if (trace)
               log.trace("Setting call recipients to " + callRecipients + " since the original list of recipients passed in is null.");
         }
         ReplicateCommand command = commandsFactory.buildReplicateCommand(call);

         List rsps = rpcManager.invokeRemotely(callRecipients,
                                               command,
                                               sync ? ResponseMode.SYNCHRONOUS : ResponseMode.ASYNCHRONOUS, // is synchronised?
                                               timeout, useOutOfBandMessage, stateTransferEnabled
         );
         if (trace) log.trace("responses=" + rsps);
         if (sync) checkResponses(rsps);
      }
   }

   /**
    * It does not make sense replicating a transaction method(commit, rollback, prepare) if one of the following is
    * true:
    * <pre>
    *    - call was not initiated here, but on other member of the cluster
    *    - there is no transaction. Why broadcast a commit or rollback if there is no transaction going on?
    *    - the current transaction did not modify any data
    * </pre>
    */
   protected final boolean skipReplicationOfTransactionMethod(InvocationContext ctx) {
      GlobalTransaction gtx = ctx.getGlobalTransaction();
      return ctx.getTransaction() == null || gtx == null || gtx.isRemote() || ctx.hasFlag(Flag.CACHE_MODE_LOCAL)
            || !ctx.getTransactionContext().hasModifications();
   }

   /**
    * The call runs in a transaction and it was initiated on this cache instance of the cluster.
    */
   protected final boolean isTransactionalAndLocal(InvocationContext ctx) {
      GlobalTransaction gtx = ctx.getGlobalTransaction();
      boolean isInitiatedHere = gtx != null && !gtx.isRemote();
      return isInitiatedHere && (ctx.getTransaction() != null);
   }

   protected final boolean isSynchronous(InvocationContext ctx) {
      if (ctx.hasFlag(Flag.FORCE_SYNCHRONOUS))
         return true;
      else if (ctx.hasFlag(Flag.FORCE_ASYNCHRONOUS))
         return false;

      return defaultSynchronous;
   }

   protected final boolean isLocalModeForced(InvocationContext ctx) {
      if (ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (log.isDebugEnabled()) log.debug("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }
}