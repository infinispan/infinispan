/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.transaction;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import java.util.Collections;
import java.util.List;

import static javax.transaction.xa.XAResource.XA_OK;
import static javax.transaction.xa.XAResource.XA_RDONLY;

/**
 * Coordinates transaction prepare/commits as received from the {@link javax.transaction.TransactionManager}.
 * Integrates with the TM through either {@link org.infinispan.transaction.xa.TransactionXaAdapter} or
 * through {@link org.infinispan.transaction.synchronization.SynchronizationAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class TransactionCoordinator {

   private static final Log log = LogFactory.getLog(TransactionCoordinator.class);
   private CommandsFactory commandsFactory;
   private InvocationContextContainer icc;
   private InterceptorChain invoker;
   private TransactionTable txTable;
   private Configuration configuration;
   private RpcManager rpcManager;

   boolean trace;

   @Inject
   public void init(CommandsFactory commandsFactory, InvocationContextContainer icc, InterceptorChain invoker,
                    TransactionTable txTable, Configuration configuration, RpcManager rpcManager) {
      this.commandsFactory = commandsFactory;
      this.icc = icc;
      this.invoker = invoker;
      this.txTable = txTable;
      this.configuration = configuration;
      trace = log.isTraceEnabled();
      this.rpcManager = rpcManager;
   }

   public int prepare(LocalTransaction localTransaction) throws XAException {
      validateNotMarkedForRollback(localTransaction);

      if (configuration.isOnePhaseCommit()) {
         if (trace) log.tracef("Received prepare for tx: %s. Skipping call as 1PC will be used.", localTransaction);
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), configuration.isOnePhaseCommit());
      if (trace) log.tracef("Sending prepare command through the chain: %s", prepareCommand);

      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, prepareCommand);
         if (localTransaction.isReadOnly()) {
            if (trace) log.tracef("Readonly transaction: %s", localTransaction.getGlobalTransaction());
            // force a cleanup to release any objects held.  Some TMs don't call commit if it is a READ ONLY tx.  See ISPN-845
            commit(localTransaction, false);
            return XA_RDONLY;
         } else {
            txTable.localTransactionPrepared(localTransaction);
            return XA_OK;
         }
      } catch (Throwable e) {
         log.error("Error while processing PrepareCommand", e);
         //rollback transaction before throwing the exception as there's no guarantee the TM calls XAResource.rollback
         //after prepare failed.
         rollback(localTransaction);
         // XA_RBROLLBACK tells the TM that we've rolled back already: the TM shouldn't call rollback after this.
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   public void commit(LocalTransaction localTransaction, boolean isOnePhase) throws XAException {
      if (trace) log.tracef("Committing transaction %s", localTransaction.getGlobalTransaction());
      try {
         LocalTxInvocationContext ctx = icc.createTxInvocationContext();
         ctx.setLocalTransaction(localTransaction);
         if (configuration.isOnePhaseCommit() || isOnePhase) {

            validateNotMarkedForRollback(localTransaction);

            if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
            PrepareCommand command = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), true);
            try {
               invoker.invoke(ctx, command);
               txTable.removeLocalTransaction(localTransaction);
            } catch (Throwable e) {
               txTable.failureCompletingTransaction(ctx.getTransaction());
               log.errorProcessing1pcPrepareCommand(e);
               throw new XAException(XAException.XAER_RMERR);
            }
         } else {
            CommitCommand commitCommand = commandsFactory.buildCommitCommand(localTransaction.getGlobalTransaction());
            try {
               invoker.invoke(ctx, commitCommand);
               txTable.removeLocalTransaction(localTransaction);
            } catch (Throwable e) {
               txTable.failureCompletingTransaction(ctx.getTransaction());
               log.errorProcessing1pcPrepareCommand(e);
               throw new XAException(XAException.XAER_RMERR);
            }
         }
      } finally {
         icc.suspend();
      }
   }

   public void rollback(LocalTransaction localTransaction) throws XAException {
      if (trace) log.tracef("rollback transaction %s ", localTransaction.getGlobalTransaction());
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, rollbackCommand);
         txTable.removeLocalTransaction(localTransaction);
      } catch (Throwable e) {
         log.errorRollingBack(e);
         final Transaction transaction = ctx.getTransaction();
         //this might be possible if the cache has stopped and TM still holds a reference to the XAResource
         if (transaction != null) {
            txTable.failureCompletingTransaction(transaction);
         }
         throw new XAException(XAException.XAER_RMERR);
      } finally {
         icc.suspend();
      }
   }

   /**
    */
   private void validateNotMarkedForRollback(LocalTransaction localTransaction) throws XAException {
      if (localTransaction.isMarkedForRollback()) {
         if (trace) log.tracef("Transaction already marked for rollback. Forcing rollback for %s", localTransaction);
         rollback(localTransaction);
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   private List<Address> getClusterMembers() {
      return rpcManager != null ? rpcManager.getTransport().getMembers() : Collections.<Address>emptyList();
   }
}
