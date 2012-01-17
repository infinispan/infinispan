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
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
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
   private CommandCreator commandCreator;
   private volatile boolean shuttingDown = false;

   boolean trace;

   @Inject
   public void init(CommandsFactory commandsFactory, InvocationContextContainer icc, InterceptorChain invoker,
                    TransactionTable txTable, Configuration configuration) {
      this.commandsFactory = commandsFactory;
      this.icc = icc;
      this.invoker = invoker;
      this.txTable = txTable;
      this.configuration = configuration;
      trace = log.isTraceEnabled();
   }

   @Start(priority = 1)
   private void setStartStatus() {
      shuttingDown = false;
   }

   @Stop(priority = 1)
   private void setStopStatus() {
      shuttingDown = true;
   }

   @Start
   public void start() {
      if (configuration.isWriteSkewCheck() && configuration.getTransactionLockingMode() == LockingMode.OPTIMISTIC
            && configuration.isEnableVersioning()) {
         // We need to create versioned variants of PrepareCommand and CommitCommand
         commandCreator = new CommandCreator() {
            @Override
            public CommitCommand createCommitCommand(GlobalTransaction gtx) {
               return commandsFactory.buildVersionedCommitCommand(gtx);
            }

            @Override
            public PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications) {
               return commandsFactory.buildVersionedPrepareCommand(gtx, modifications, false);
            }
         };
      } else {
         commandCreator = new CommandCreator() {
            @Override
            public CommitCommand createCommitCommand(GlobalTransaction gtx) {
               return commandsFactory.buildCommitCommand(gtx);
            }

            @Override
            public PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications) {
               return commandsFactory.buildPrepareCommand(gtx, modifications, false);
            }
         };
      }
   }

   public final int prepare(LocalTransaction localTransaction) throws XAException {
      return prepare(localTransaction, false);
   }

   public final int prepare(LocalTransaction localTransaction, boolean replayEntryWrapping) throws XAException {
      validateNotMarkedForRollback(localTransaction);

      if (configuration.isOnePhaseCommit() || is1PcForAutoCommitTransaction(localTransaction)) {
         if (trace) log.tracef("Received prepare for tx: %s. Skipping call as 1PC will be used.", localTransaction);
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandCreator.createPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications());
      if (trace) log.tracef("Sending prepare command through the chain: %s", prepareCommand);

      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      prepareCommand.setReplayEntryWrapping(replayEntryWrapping);
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
         if (shuttingDown)
            log.trace("Exception while preparing back, probably because we're shutting down.");
         else
            log.error("Error while processing prepare", e);

         //rollback transaction before throwing the exception as there's no guarantee the TM calls XAResource.rollback
         //after prepare failed.
         rollback(localTransaction);
         // XA_RBROLLBACK tells the TM that we've rolled back already: the TM shouldn't call rollback after this.
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   public void commit(LocalTransaction localTransaction, boolean isOnePhase) throws XAException {
      if (trace) log.tracef("Committing transaction %s", localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      if (configuration.isOnePhaseCommit() || isOnePhase || is1PcForAutoCommitTransaction(localTransaction)) {
         validateNotMarkedForRollback(localTransaction);

         if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
         PrepareCommand command = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), true);
         try {
            invoker.invoke(ctx, command);
         } catch (Throwable e) {
            handleCommitFailure(e, localTransaction);
         }
      } else {
         CommitCommand commitCommand = commandCreator.createCommitCommand(localTransaction.getGlobalTransaction());
         try {
            invoker.invoke(ctx, commitCommand);
            txTable.removeLocalTransaction(localTransaction);
         } catch (Throwable e) {
            handleCommitFailure(e, localTransaction);
         }
      }
   }

   public void rollback(LocalTransaction localTransaction) throws XAException {
      try {
         rollbackInternal(localTransaction);
      } catch (Throwable e) {
         if (shuttingDown)
            log.trace("Exception while rolling back, probably because we're shutting down.");
         else
            log.errorRollingBack(e);

         final Transaction transaction = localTransaction.getTransaction();
         //this might be possible if the cache has stopped and TM still holds a reference to the XAResource
         if (transaction != null) {
            txTable.failureCompletingTransaction(transaction);
         }
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   private void handleCommitFailure(Throwable e, LocalTransaction localTransaction) throws XAException {
      log.errorProcessing1pcPrepareCommand(e);
      if (trace) log.tracef("Couldn't commit 1PC transaction %s, trying to rollback.", localTransaction);
      try {
         rollbackInternal(localTransaction);
      } catch (Throwable e1) {
         log.couldNotRollbackPrepared1PcTransaction(localTransaction, e1);
         // inform the TM that a resource manager error has occurred in the transaction branch (XAER_RMERR).
         throw new XAException(XAException.XAER_RMERR);
      } finally {
         txTable.failureCompletingTransaction(localTransaction.getTransaction());
      }
      throw new XAException(XAException.XA_HEURRB); //this is a heuristic rollback
   }

   private void rollbackInternal(LocalTransaction localTransaction) throws Throwable {
      if (trace) log.tracef("rollback transaction %s ", localTransaction.getGlobalTransaction());
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      invoker.invoke(ctx, rollbackCommand);
      txTable.removeLocalTransaction(localTransaction);
   }

   private void validateNotMarkedForRollback(LocalTransaction localTransaction) throws XAException {
      if (localTransaction.isMarkedForRollback()) {
         if (trace) log.tracef("Transaction already marked for rollback. Forcing rollback for %s", localTransaction);
         rollback(localTransaction);
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   private boolean is1PcForAutoCommitTransaction(LocalTransaction localTransaction) {
      return configuration.isUse1PcForAutoCommitTransactions() && localTransaction.isImplicitTransaction();
   }

   private static interface CommandCreator {
      CommitCommand createCommitCommand(GlobalTransaction gtx);
      PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications);
   }
}
