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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.newstatetransfer.StateTransferLock;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Takes care of replicating modifications to other caches in a cluster.
 *
 * @author Bela Ban
 * @since 4.0
 */
public class ReplicationInterceptor extends BaseRpcInterceptor {

   CommandsFactory cf;

   private static final Log log = LogFactory.getLog(ReplicationInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(CommandsFactory cf) {
      this.cf = cf;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isInTxScope()) throw new IllegalStateException("This should not be possible!");
      if (shouldInvokeRemoteTxCommand(ctx)) {
         sendCommitCommand(ctx, command);
      }
      return invokeNextInterceptor(ctx, command);
   }

   /**
    * If the response to a commit is a request to resend the prepare, respond accordingly *
    */
   private boolean needToResendPrepare(Response r) {
      return r instanceof SuccessfulResponse && Byte.valueOf(CommitCommand.RESEND_PREPARE).equals(((SuccessfulResponse) r).getResponseValue());
   }

   private void sendCommitCommand(TxInvocationContext ctx, CommitCommand command)
         throws TimeoutException, InterruptedException {
      // may need to resend, so make the commit command synchronous
      // TODO keep the list of prepared nodes or the view id when the prepare command was sent to know whether we need to resend the prepare info
      Map<Address, Response> responses = rpcManager.invokeRemotely(null, command, cacheConfiguration.transaction().syncCommitPhase(), true);
      if (!responses.isEmpty()) {
         List<Address> resendTo = new LinkedList<Address>();
         for (Map.Entry<Address, Response> r : responses.entrySet()) {
            if (needToResendPrepare(r.getValue()))
               resendTo.add(r.getKey());
         }

         if (!resendTo.isEmpty()) {
            getLog().debugf("Need to resend prepares for %s to %s", command.getGlobalTransaction(), resendTo);
            PrepareCommand pc = buildPrepareCommandForResend(ctx, command);
            rpcManager.invokeRemotely(resendTo, pc, true, true);
         }
      }
   }

   protected PrepareCommand buildPrepareCommandForResend(TxInvocationContext ctx, CommitCommand command) {
      // Make sure this is 1-Phase!!
      return cf.buildPrepareCommand(command.getGlobalTransaction(), ctx.getModifications(), true);
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
      populateCommandFlags(command, ctx);
      if (!isLocalModeForced(ctx) && command.isSuccessful() && ctx.isOriginLocal() && !ctx.isInTxScope()) {
         if (ctx.isUseFutureReturnType()) {
            NotifyingNotifiableFuture<Object> future = new NotifyingFutureImpl(returnValue);
            rpcManager.broadcastRpcCommandInFuture(command, future);
            return future;
         } else {
            rpcManager.broadcastRpcCommand(command, isSynchronous(ctx));
         }
      }
      return returnValue;
   }

   /**
    * Makes sure the context Flags are bundled in the command, so that they are re-read remotely
    */
   private void populateCommandFlags(WriteCommand command, InvocationContext ctx) {
      command.setFlags(ctx.getFlags());
   }
}
