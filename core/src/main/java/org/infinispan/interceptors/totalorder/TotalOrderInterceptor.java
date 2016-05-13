package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 * @author Pedro Ruivo
 * @author Mircea.Markus@jboss.com
 */
public class TotalOrderInterceptor extends DDSequentialInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private TransactionTable transactionTable;
   private TotalOrderManager totalOrderManager;
   private ClusteringDependentLogic clusteringDependentLogic;
   private BlockingTaskAwareExecutorService executorService;

   @Inject
   public void inject(TransactionTable transactionTable, TotalOrderManager totalOrderManager,
                      ClusteringDependentLogic clusteringDependentLogic,
                      @ComponentName(value = KnownComponentNames.REMOTE_COMMAND_EXECUTOR)
                      BlockingTaskAwareExecutorService executorService) {
      this.transactionTable = transactionTable;
      this.totalOrderManager = totalOrderManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.executorService = executorService;
   }

   @Override
   public final CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (log.isDebugEnabled()) {
         log.debugf("Prepare received. Transaction=%s, Affected keys=%s, Local=%s",
                    command.getGlobalTransaction().globalId(),
                    toStr(command.getAffectedKeys()),
                    ctx.isOriginLocal());
      }
      if (!(command instanceof TotalOrderPrepareCommand)) {
         throw new IllegalStateException("TotalOrderInterceptor can only handle TotalOrderPrepareCommand");
      }

      try {
         simulateLocking(ctx, command, clusteringDependentLogic);
         if (ctx.isOriginLocal()) {
            return ctx.shortCircuit(ctx.forkInvocationSync(command));
         } else {
            TotalOrderRemoteTransactionState state = getTransactionState(ctx);

            try {
               state.preparing();
               if (state.isRollbackReceived()) {
                  //this means that rollback has already been received
                  transactionTable.removeRemoteTransaction(command.getGlobalTransaction());
                  throw new CacheException("Cannot prepare transaction" + command.getGlobalTransaction().globalId() +
                                                 ". it was already marked as rollback");
               }

               if (state.isCommitReceived()) {
                  log.tracef("Transaction %s marked for commit, skipping the write skew check and forcing 1PC",
                             command.getGlobalTransaction().globalId());
                  ((TotalOrderPrepareCommand) command).markSkipWriteSkewCheck();
                  ((TotalOrderPrepareCommand) command).markAsOnePhaseCommit();
               }


               if (trace) {
                  log.tracef("Validating transaction %s ", command.getGlobalTransaction().globalId());
               }

               //invoke next interceptor in the chain
               Object result = ctx.forkInvocationSync(command);

               if (command.isOnePhaseCommit()) {
                  totalOrderManager.release(state);
               }
               return ctx.shortCircuit(result);
            } finally {
               state.prepared();
            }

         }
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while preparing for transaction %s. Local=%s",
                       command.getGlobalTransaction().globalId(), ctx.isOriginLocal());
         }
         if (command.isOnePhaseCommit()) {
            transactionTable.remoteTransactionRollback(command.getGlobalTransaction());
         }
         throw exception;
      }
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, false);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, true);
   }

   @Override
   public final CompletableFuture<Void> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
   }

   private CompletableFuture<Void> visitSecondPhaseCommand(TxInvocationContext context, AbstractTransactionBoundaryCommand command, boolean commit) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();
      if (trace) {
         log.tracef("Second phase command received. Commit?=%s Transaction=%s, Local=%s", commit, gtx.globalId(),
                    context.isOriginLocal());
      }

      TotalOrderRemoteTransactionState state = getTransactionState(context);

      try {
         if (!processSecondCommand(state, commit) && !context.isOriginLocal()) {
            //we can return here, because we set onePhaseCommit to prepare and it will release all the resources
            return context.shortCircuit(null);
         }

         return context.shortCircuit(context.forkInvocationSync(command));
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while rollback transaction %s", gtx.globalId());
         }
         throw exception;
      } finally {
         if (state != null && state.isFinished()) {
            totalOrderManager.release(state);
            if (commit) {
               transactionTable.remoteTransactionCommitted(command.getGlobalTransaction(), false);
            } else {
               transactionTable.remoteTransactionRollback(command.getGlobalTransaction());
            }
            if (context.isOriginLocal()) {
               executorService.checkForReadyTasks();
            }
         }
      }
   }

   private TotalOrderRemoteTransactionState getTransactionState(TxInvocationContext context) {
      if (!context.isOriginLocal()) {
         return ((RemoteTransaction) context.getCacheTransaction()).getTransactionState();
      }
      RemoteTransaction remoteTransaction = transactionTable.getRemoteTransaction(context.getGlobalTransaction());
      return remoteTransaction == null ? null : remoteTransaction.getTransactionState();
   }

   private boolean processSecondCommand(TotalOrderRemoteTransactionState state, boolean commit) {
      //it means that the transaction was not prepared (i.e. a suicide transaction)
      if (state == null) {
         return true;
      }

      try {
         return state.waitUntilPrepared(commit);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.timeoutWaitingUntilTransactionPrepared(state.getGlobalTransaction().globalId());
      }
      return false;
   }

   private void simulateLocking(TxInvocationContext context, PrepareCommand command,
                                ClusteringDependentLogic clusteringDependentLogic) {
      Collection<Object> affectedKeys = command.getAffectedKeys();
      //this map is only populated after locks are acquired. However, no locks are acquired when total order is enabled
      //so we need to populate it here
      context.addAllAffectedKeys(command.getAffectedKeys());
      //prepare can be send more than once if we have a state transfer
      context.clearLockedKeys();
      for (Object k : affectedKeys) {
         if (clusteringDependentLogic.localNodeIsPrimaryOwner(k)) {
            context.addLockedKey(k);
         }
      }
   }
}
