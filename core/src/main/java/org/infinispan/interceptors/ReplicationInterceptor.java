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

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

/**
 * Takes care of replicating modifications to other caches in a cluster. Also listens for prepare(), commit() and
 * rollback() messages which are received 'side-ways' (see docs/design/Refactoring.txt).
 *
 * @author Bela Ban
 * @since 4.0
 */
public class ReplicationInterceptor extends BaseRpcInterceptor {

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isInTxScope()) throw new IllegalStateException("This should not be possible!");
      if (shouldInvokeRemoteTxCommand(ctx)) {
         rpcManager.broadcastRpcCommand(command, configuration.isSyncCommitPhase(), true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         boolean async = configuration.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
         rpcManager.broadcastRpcCommand(command, !async, false);
      }
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx) && !configuration.isOnePhaseCommit()) {
         rpcManager.broadcastRpcCommand(command, configuration.isSyncRollbackPhase(), true);
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
