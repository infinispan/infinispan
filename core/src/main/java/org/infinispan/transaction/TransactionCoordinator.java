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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAException;

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
   Configuration configuration;

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

   public int prepare(LocalTransaction localTransaction) throws XAException {
      validateNotMarkedForRollback(localTransaction);

      if (configuration.isOnePhaseCommit()) {
         if (trace) log.trace("Received prepare for tx: %s. Skipping call as 1PC will be used.", localTransaction);
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), configuration.isOnePhaseCommit());
      if (trace) log.trace("Sending prepare command through the chain: " + prepareCommand);

      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, prepareCommand);
         if (localTransaction.isReadOnly()) {
            if (trace) log.trace("Readonly transaction: " + localTransaction.getGlobalTransaction());
            // force a cleanup to release any objects held.  Some TMs don't call commit if it is a READ ONLY tx.  See ISPN-845
            commit(localTransaction, false);
            return XA_RDONLY;
         } else {
            txTable.localTransactionPrepared(localTransaction);
            return XA_OK;
         }
      } catch (Throwable e) {
         log.error("Error while processing PrepareCommand", e);
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   private void validateNotMarkedForRollback(LocalTransaction localTransaction) throws XAException {
      if (localTransaction.isMarkedForRollback()) {
         if (trace) log.trace("Transaction already marked for rollback: %s", localTransaction);
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   public void commit(LocalTransaction localTransaction, boolean isOnePhase) throws XAException {
      if (trace) log.trace("Committing transaction %s", localTransaction.getGlobalTransaction());
      try {
         LocalTxInvocationContext ctx = icc.createTxInvocationContext();
         ctx.setLocalTransaction(localTransaction);
         if (configuration.isOnePhaseCommit() || isOnePhase) {
            validateNotMarkedForRollback(localTransaction);

            if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
            PrepareCommand command = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), true);
            try {
               invoker.invoke(ctx, command);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         } else {
            CommitCommand commitCommand = commandsFactory.buildCommitCommand(localTransaction.getGlobalTransaction());
            try {
               invoker.invoke(ctx, commitCommand);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         }
      } finally {
         cleanupImpl(localTransaction, txTable, icc);
      }
   }

   public void rollback(LocalTransaction localTransaction) throws XAException {
      if (trace) log.trace("rollback transaction %s ", localTransaction.getGlobalTransaction());
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, rollbackCommand);
      } catch (Throwable e) {
         log.error("Exception while rollback", e);
         throw new XAException(XAException.XA_HEURHAZ);
      } finally {
         cleanupImpl(localTransaction, txTable, icc);
      }
   }

   private static void cleanupImpl(LocalTransaction localTransaction, TransactionTable txTable, InvocationContextContainer icc) {
      txTable.removeLocalTransaction(localTransaction);
      icc.suspend();
   }
}
