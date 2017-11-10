package org.infinispan.interceptors.totalorder;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Collection;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 * @author Pedro Ruivo
 * @author Mircea.Markus@jboss.com
 */
public class TotalOrderInterceptor extends DDAsyncInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private TransactionTable transactionTable;
   @Inject private TotalOrderManager totalOrderManager;
   @Inject private ClusteringDependentLogic clusteringDependentLogic;
   @Inject @ComponentName(value = KnownComponentNames.REMOTE_COMMAND_EXECUTOR)
   private BlockingTaskAwareExecutorService executorService;

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command)
         throws Throwable {
      if (log.isDebugEnabled()) {
         log.debugf("Prepare received. Transaction=%s, Affected keys=%s, Local=%s",
                    command.getGlobalTransaction().globalId(),
                    toStr(command.getAffectedKeys()),
                    ctx.isOriginLocal());
      }
      if (!(command instanceof TotalOrderPrepareCommand)) {
         throw new IllegalStateException("TotalOrderInterceptor can only handle TotalOrderPrepareCommand");
      }

      TotalOrderRemoteTransactionState state = getTransactionState(ctx);

      try {
         simulateLocking(ctx, command, clusteringDependentLogic);

         if (ctx.isOriginLocal()) {
            return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
               if (t != null) {
                  rollbackTxOnPrepareException(rCtx, (PrepareCommand) rCommand, t);
               }
            });
         }

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

         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
            afterPrepare((TxInvocationContext) rCtx, (PrepareCommand) rCommand, state, t);
         });
      } catch (Throwable t) {
         afterPrepare(ctx, command, state, t);
         throw t;
      }
   }

   private void rollbackTxOnPrepareException(InvocationContext ctx, PrepareCommand command, Throwable throwable) {
      if (log.isDebugEnabled()) {
         log.debugf(throwable, "Exception while preparing for transaction %s. Local=%s",
               command.getGlobalTransaction().globalId(), ctx.isOriginLocal());
      }
      if (command.isOnePhaseCommit()) {
         transactionTable.remoteTransactionRollback(command.getGlobalTransaction());
      }
   }

   private void afterPrepare(TxInvocationContext ctx, PrepareCommand command, TotalOrderRemoteTransactionState state,
         Throwable t) {
      if (t == null && command.isOnePhaseCommit()) {
         totalOrderManager.release(state);
      }

      state.prepared();

      if (t != null) {
         rollbackTxOnPrepareException(ctx, command, t);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, false);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, true);
   }

   @Override
   public final Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
   }

   private Object visitSecondPhaseCommand(TxInvocationContext context,
         AbstractTransactionBoundaryCommand command, boolean commit) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();
      if (trace) {
         log.tracef("Second phase command received. Commit?=%s Transaction=%s, Local=%s", commit, gtx.globalId(),
               context.isOriginLocal());
      }

      TotalOrderRemoteTransactionState state = getTransactionState(context);
      try {
         if (!processSecondCommand(state, commit) && !context.isOriginLocal()) {
            //we can return here, because we set onePhaseCommit to prepare and it will release all the resources
            return null;
         }
      } catch (Throwable t) {
         finishSecondPhaseCommand(commit, state, context, command);
         throw t;
      }

      return invokeNextAndFinally(context, command, (rCtx, rCommand, rv, t) ->
            finishSecondPhaseCommand(commit, state, rCtx, (AbstractTransactionBoundaryCommand) rCommand));
   }

   private void finishSecondPhaseCommand(boolean commit, TotalOrderRemoteTransactionState state, InvocationContext ctx,
         AbstractTransactionBoundaryCommand txCommand) {
      if (state != null && state.isFinished()) {
         totalOrderManager.release(state);
         if (commit) {
            transactionTable.remoteTransactionCommitted(txCommand.getGlobalTransaction(), false);
         } else {
            transactionTable.remoteTransactionRollback(txCommand.getGlobalTransaction());
         }
         if (ctx.isOriginLocal()) {
            executorService.checkForReadyTasks();
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
      Collection<?> affectedKeys = command.getAffectedKeys();
      //this map is only populated after locks are acquired. However, no locks are acquired when total order is enabled
      //so we need to populate it here
      context.addAllAffectedKeys(command.getAffectedKeys());
      //prepare can be send more than once if we have a state transfer
      context.clearLockedKeys();
      for (Object k : affectedKeys) {
         if (clusteringDependentLogic.getCacheTopology().getDistribution(k).isPrimary()) {
            context.addLockedKey(k);
         }
      }
   }
}
