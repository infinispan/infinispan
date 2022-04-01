package org.infinispan.transaction.impl;

import static javax.transaction.xa.XAResource.XA_OK;
import static javax.transaction.xa.XAResource.XA_RDONLY;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Coordinates transaction prepare/commits as received from the {@link javax.transaction.TransactionManager}.
 * Integrates with the TM through either {@link org.infinispan.transaction.xa.TransactionXaAdapter} or
 * through {@link org.infinispan.transaction.synchronization.SynchronizationAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
public class TransactionCoordinator {
   private static final Log log = LogFactory.getLog(TransactionCoordinator.class);

   @Inject CommandsFactory commandsFactory;
   @Inject ComponentRef<InvocationContextFactory> icf;
   @Inject ComponentRef<AsyncInterceptorChain> invoker;
   @Inject ComponentRef<TransactionTable> txTable;
   @Inject ComponentRef<RecoveryManager> recoveryManager;
   @Inject Configuration configuration;

   private CommandCreator commandCreator;
   private volatile boolean shuttingDown = false;

   private boolean defaultOnePhaseCommit;
   private boolean use1PcForAutoCommitTransactions;

   private static final CompletableFuture<Integer> XA_OKAY_STAGE = CompletableFuture.completedFuture(XA_OK);
   private static final Function<Object, Integer> XA_RDONLY_APPLY = ignore -> XA_RDONLY;

   @Start(priority = 1)
   void setStartStatus() {
      shuttingDown = false;
   }

   @Stop(priority = 1)
   void setStopStatus() {
      shuttingDown = true;
   }

   @Start
   public void start() {
      use1PcForAutoCommitTransactions = configuration.transaction().use1PcForAutoCommitTransactions();
      defaultOnePhaseCommit = Configurations.isOnePhaseCommit(configuration);

      if (Configurations.isTxVersioned(configuration)) {
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

   public final CompletionStage<Integer> prepare(LocalTransaction localTransaction) {
      return prepare(localTransaction, false);
   }

   public final CompletionStage<Integer> prepare(LocalTransaction localTransaction, boolean replayEntryWrapping) {
      CompletionStage<Integer> markRollbackStage = validateNotMarkedForRollback(localTransaction);
      if (markRollbackStage != null) {
         return markRollbackStage;
      }

      if (isOnePhaseCommit(localTransaction)) {
         if (log.isTraceEnabled()) log.tracef("Received prepare for tx: %s. Skipping call as 1PC will be used.", localTransaction);
         return XA_OKAY_STAGE;
      }

      PrepareCommand prepareCommand = commandCreator.createPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), false);
      if (log.isTraceEnabled()) log.tracef("Sending prepare command through the chain: %s", prepareCommand);

      LocalTxInvocationContext ctx = icf.running().createTxInvocationContext(localTransaction);
      prepareCommand.setReplayEntryWrapping(replayEntryWrapping);
      CompletionStage<Object> prepareStage = invoker.running().invokeAsync(ctx, prepareCommand);
      return CompletionStages.handleAndCompose(prepareStage, (ignore, prepareThrowable) -> {
         if (prepareThrowable != null) {
            if (shuttingDown)
               log.trace("Exception while preparing back, probably because we're shutting down.");
            else
               log.errorProcessingPrepare(prepareThrowable);

            //rollback transaction before throwing the exception as there's no guarantee the TM calls XAResource.rollback
            //after prepare failed.
            return CompletionStages.handleAndCompose(rollback(localTransaction), (ignore2, rollbackThrowable) -> {
               // XA_RBROLLBACK tells the TM that we've rolled back already: the TM shouldn't call rollback after this.
               XAException xe = new XAException(XAException.XA_RBROLLBACK);
               if (rollbackThrowable != null) {
                  rollbackThrowable.addSuppressed(prepareThrowable);
                  xe.initCause(rollbackThrowable);
               } else {
                  xe.initCause(prepareThrowable);
               }
               return CompletableFutures.completedExceptionFuture(xe);
            });
         }
         if (localTransaction.isReadOnly()) {
            if (log.isTraceEnabled()) log.tracef("Readonly transaction: %s", localTransaction.getGlobalTransaction());
            // force a cleanup to release any objects held.  Some TMs don't call commit if it is a READ ONLY tx.  See ISPN-845
            return commitInternal(ctx)
                  .thenApply(XA_RDONLY_APPLY);
         } else {
            txTable.running().localTransactionPrepared(localTransaction);
            return XA_OKAY_STAGE;
         }
      });
   }

   public CompletionStage<Boolean> commit(LocalTransaction localTransaction, boolean isOnePhase) {
      if (log.isTraceEnabled()) log.tracef("Committing transaction %s", localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icf.running().createTxInvocationContext(localTransaction);
      if (isOnePhaseCommit(localTransaction) || isOnePhase) {
         CompletionStage<Boolean> markRollbackStage = validateNotMarkedForRollback(localTransaction);
         if (markRollbackStage != null) {
            return markRollbackStage;
         }

         if (log.isTraceEnabled()) log.trace("Doing an 1PC prepare call on the interceptor chain");
         List<WriteCommand> modifications = localTransaction.getModifications();
         PrepareCommand command = commandCreator.createPrepareCommand(localTransaction.getGlobalTransaction(), modifications, true);
         return CompletionStages.handleAndCompose(invoker.running().invokeAsync(ctx, command),
               (ignore, t) -> {
                  if (t != null) {
                     return handleCommitFailure(t, true, ctx);
                  }
                  return CompletableFutures.completedTrue();
               });
      } else if (!localTransaction.isReadOnly()) {
         return commitInternal(ctx);
      }
      return CompletableFutures.completedFalse();
   }

   public CompletionStage<Void> rollback(LocalTransaction localTransaction) {
      return CompletionStages.handleAndCompose(rollbackInternal(icf.running().createTxInvocationContext(localTransaction)),
            (ignore, t) -> {
               if (t != null) {
                  return handleRollbackFailure(t, localTransaction);
               }
               return CompletableFutures.completedNull();
            });
   }

   private <T> CompletionStage<T> handleRollbackFailure(Throwable t, LocalTransaction localTransaction) {
      if (shuttingDown)
         log.trace("Exception while rolling back, probably because we're shutting down.");
      else
         log.errorRollingBack(t);

      final Transaction transaction = localTransaction.getTransaction();
      //this might be possible if the cache has stopped and TM still holds a reference to the XAResource
      if (transaction != null) {
         txTable.running().failureCompletingTransaction(transaction);
      }
      XAException xe = new XAException(XAException.XAER_RMERR);
      xe.initCause(t);
      return CompletableFutures.completedExceptionFuture(t);
   }

   private <T> CompletionStage<T> handleCommitFailure(Throwable e, boolean onePhaseCommit, LocalTxInvocationContext ctx) {
      if (log.isTraceEnabled()) log.tracef("Couldn't commit transaction %s, trying to rollback.", ctx.getCacheTransaction());
      if (onePhaseCommit) {
         log.errorProcessing1pcPrepareCommand(e);
      } else {
         log.errorProcessing2pcCommitCommand(e);
      }
      boolean isRecoveryEnabled = recoveryManager.running() != null;
      CompletionStage<Void> stage;
      if (!isRecoveryEnabled) {
         //the rollback is not needed any way, because if one node aborts the transaction, then all the nodes will
         //abort too.
         stage = rollbackInternal(ctx);
      } else {
         stage = CompletableFutures.completedNull();
      }
      return stage.handle((ignore, t) -> {
         txTable.running().failureCompletingTransaction(ctx.getTransaction());
         if (t != null) {
            log.couldNotRollbackPrepared1PcTransaction(ctx.getCacheTransaction(), t);
            // inform the TM that a resource manager error has occurred in the transaction branch (XAER_RMERR).
            XAException xe = new XAException(XAException.XAER_RMERR);
            xe.initCause(t);
            xe.addSuppressed(e);
            throw new CompletionException(xe);
         }

         XAException xe = new XAException(XAException.XA_HEURRB);
         xe.initCause(e);
         throw new CompletionException(xe); //this is a heuristic rollback
      });
   }

   private CompletionStage<Boolean> commitInternal(LocalTxInvocationContext ctx) {
      CommitCommand commitCommand = commandCreator.createCommitCommand(ctx.getGlobalTransaction());
      CompletableFuture<Object> commitStage = invoker.running().invokeAsync(ctx, commitCommand);
      return CompletionStages.handleAndCompose(commitStage, (ignore, t) -> {
         if (t != null) {
            return handleCommitFailure(t, false, ctx);
         }
         txTable.running().removeLocalTransaction(ctx.getCacheTransaction());
         return CompletableFutures.completedFalse();
      });
   }

   private CompletionStage<Void> rollbackInternal(LocalTxInvocationContext ctx) {
      if (log.isTraceEnabled()) log.tracef("rollback transaction %s ", ctx.getGlobalTransaction());
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(ctx.getGlobalTransaction());
      return invoker.running().invokeAsync(ctx, rollbackCommand)
            .thenRun(() ->
                  txTable.running().removeLocalTransaction(ctx.getCacheTransaction())
            );
   }

   private <T> CompletionStage<T> validateNotMarkedForRollback(LocalTransaction localTransaction) {
      if (localTransaction.isMarkedForRollback()) {
         if (log.isTraceEnabled()) log.tracef("Transaction already marked for rollback. Forcing rollback for %s", localTransaction);
         return rollback(localTransaction).thenApply(ignore -> {
            throw CompletableFutures.asCompletionException(new XAException(XAException.XA_RBROLLBACK));
         });
      }
      return null;
   }

   public boolean is1PcForAutoCommitTransaction(LocalTransaction localTransaction) {
      return use1PcForAutoCommitTransactions && localTransaction.isImplicitTransaction();
   }

   private interface CommandCreator {
      CommitCommand createCommitCommand(GlobalTransaction gtx);
      PrepareCommand createPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit);
   }

   private boolean isOnePhaseCommit(LocalTransaction localTransaction) {
      return defaultOnePhaseCommit || is1PcForAutoCommitTransaction(localTransaction);
   }
}
