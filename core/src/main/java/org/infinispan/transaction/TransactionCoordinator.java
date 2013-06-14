package org.infinispan.transaction;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
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
 * @author Pedro Ruivo
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
      if (Configurations.isVersioningEnabled(configuration)) {
         // We need to create versioned variants of PrepareCommand and CommitCommand
         commandCreator = new CommandCreator() {
            @Override
            public CommitCommand createCommitCommand(GlobalTransaction gtx) {
               return commandsFactory.buildVersionedCommitCommand(gtx);
            }

            @Override
            public PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
               return commandsFactory.buildVersionedPrepareCommand(gtx, modifications, onePhaseCommit);
            }
         };
      } else {
         commandCreator = new CommandCreator() {
            @Override
            public CommitCommand createCommitCommand(GlobalTransaction gtx) {
               return commandsFactory.buildCommitCommand(gtx);
            }

            @Override
            public PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
               return commandsFactory.buildPrepareCommand(gtx, modifications, onePhaseCommit);
            }
         };
      }
   }

   public final int prepare(LocalTransaction localTransaction) throws XAException {
      return prepare(localTransaction, false);
   }

   public final int prepare(LocalTransaction localTransaction, boolean replayEntryWrapping) throws XAException {
      validateNotMarkedForRollback(localTransaction);

      if (isOnePhaseCommit(localTransaction)) {
         if (trace) log.tracef("Received prepare for tx: %s. Skipping call as 1PC will be used.", localTransaction);
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandCreator.createPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), false);
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

   public boolean commit(LocalTransaction localTransaction, boolean isOnePhase) throws XAException {
      if (trace) log.tracef("Committing transaction %s", localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      if (isOnePhaseCommit(localTransaction) || isOnePhase) {
         validateNotMarkedForRollback(localTransaction);

         if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
         List<WriteCommand> modifications = localTransaction.getModifications();
         PrepareCommand command = commandCreator.createPrepareCommand(localTransaction.getGlobalTransaction(), modifications, true);
         try {
            invoker.invoke(ctx, command);
         } catch (Throwable e) {
            handleCommitFailure(e, localTransaction, true);
         }
         return true;
      } else {
         CommitCommand commitCommand = commandCreator.createCommitCommand(localTransaction.getGlobalTransaction());
         try {
            invoker.invoke(ctx, commitCommand);
            txTable.removeLocalTransaction(localTransaction);
         } catch (Throwable e) {
            handleCommitFailure(e, localTransaction, false);
         }
         return false;
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

   private void handleCommitFailure(Throwable e, LocalTransaction localTransaction, boolean onePhaseCommit) throws XAException {
      if (trace) log.tracef("Couldn't commit transaction %s, trying to rollback.", localTransaction);
      if (onePhaseCommit) {
         log.errorProcessing1pcPrepareCommand(e);
      } else {
         log.errorProcessing2pcCommitCommand(e);
      }
      try {
         if (!(onePhaseCommit && configuration.transaction().transactionProtocol().isTotalOrder())) {
            //we cannot send the rollback in Total Order because it will create a new remote transaction.
            //the rollback is not needed any way, because if one node aborts the transaction, then all the nodes will
            //abort too.
            rollbackInternal(localTransaction);
         }
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

   public boolean is1PcForAutoCommitTransaction(LocalTransaction localTransaction) {
      return configuration.transaction().use1PcForAutoCommitTransactions() && localTransaction.isImplicitTransaction();
   }

   private static interface CommandCreator {
      CommitCommand createCommitCommand(GlobalTransaction gtx);
      PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit);
   }

   private boolean isOnePhaseCommit(LocalTransaction localTransaction) {
      return Configurations.isOnePhaseCommit(configuration) || is1PcForAutoCommitTransaction(localTransaction) ||
            Configurations.isOnePhaseTotalOrderCommit(configuration);
   }
}
