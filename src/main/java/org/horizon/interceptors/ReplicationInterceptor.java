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
package org.horizon.interceptors;

import org.horizon.commands.tx.CommitCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.tx.RollbackCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.commands.write.WriteCommand;
import org.horizon.config.Configuration;
import org.horizon.context.InvocationContext;
import org.horizon.context.TransactionContext;
import org.horizon.interceptors.base.BaseRpcInterceptor;
import org.horizon.transaction.GlobalTransaction;

/**
 * Takes care of replicating modifications to other caches in a cluster. Also listens for prepare(), commit() and
 * rollback() messages which are received 'side-ways' (see docs/design/Refactoring.txt).
 *
 * @author Bela Ban
 * @since 1.0
 */
public class ReplicationInterceptor extends BaseRpcInterceptor {

   @Override
   public Object visitCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      if (!skipReplicationOfTransactionMethod(ctx))
         replicateCall(ctx, command, configuration.isSyncCommitPhase(), true);
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      TransactionContext transactionContext = ctx.getTransactionContext();
      if (transactionContext.hasLocalModifications()) {
         PrepareCommand replicablePrepareCommand = command.copy(); // make sure we remove any "local" transactions
         replicablePrepareCommand.removeModifications(transactionContext.getLocalModifications());
         command = replicablePrepareCommand;
      }

      if (!skipReplicationOfTransactionMethod(ctx)) runPreparePhase(command, command.getGlobalTransaction(), ctx);
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!skipReplicationOfTransactionMethod(ctx) && !ctx.isLocalRollbackOnly()) {
         replicateCall(ctx, command, configuration.isSyncRollbackPhase());
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
   private Object handleCrudMethod(InvocationContext ctx, WriteCommand command) throws Throwable {
      boolean local = isLocalModeForced(ctx);
      if (local && ctx.getTransaction() == null) return invokeNextInterceptor(ctx, command);
      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to replicate.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {
         if (ctx.getTransaction() == null && ctx.isOriginLocal()) {
            if (trace) {
               log.trace("invoking method " + command.getClass().getSimpleName() + ", members=" + rpcManager.getTransport().getMembers() + ", mode=" +
                     configuration.getCacheMode() + ", exclude_self=" + true + ", timeout=" +
                     configuration.getSyncReplTimeout());
            }

            replicateCall(ctx, command, isSynchronous(ctx));
         } else {
            if (local) ctx.getTransactionContext().addLocalModification(command);
         }
      }
      return returnValue;
   }

   /**
    * Calls prepare(GlobalTransaction,List,Address,boolean)) in all members except self. Waits for all responses. If one
    * of the members failed to prepare, its return value will be an exception. If there is one exception we rethrow it.
    * This will mark the current transaction as rolled back, which will cause the afterCompletion(int) callback to have
    * a status of <tt>MARKED_ROLLBACK</tt>. When we get that call, we simply roll back the transaction.<br/> If
    * everything runs okay, the afterCompletion(int) callback will trigger the @link
    * #runCommitPhase(GlobalTransaction)). <br/>
    *
    * @throws Exception
    */
   protected void runPreparePhase(PrepareCommand prepareMethod, GlobalTransaction gtx, InvocationContext ctx) throws Throwable {
      boolean async = configuration.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      if (trace) {
         log.trace("(" + rpcManager.getTransport().getAddress() + "): running remote prepare for global tx " + gtx + " with async mode=" + async);
      }

      // this method will return immediately if we're the only member (because exclude_self=true)
      replicateCall(ctx, prepareMethod, !async);
   }
}
