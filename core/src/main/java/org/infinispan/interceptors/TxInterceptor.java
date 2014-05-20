package org.infinispan.interceptors;

import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoverableTransactionIdentifier;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with handling transaction related operations, e.g enlisting cache as an transaction
 * participant, propagating remotely initiated changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.transaction.xa.TransactionXaAdapter
 * @since 4.0
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class TxInterceptor extends CommandInterceptor {

   private TransactionTable txTable;

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);
   private boolean statisticsEnabled;
   protected TransactionCoordinator txCoordinator;
   protected RpcManager rpcManager;

   private static final Log log = LogFactory.getLog(TxInterceptor.class);
   private RecoveryManager recoveryManager;
   private boolean isTotalOrder;
   private boolean useOnePhaseForAutoCommitTx;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(TransactionTable txTable, Configuration c, TransactionCoordinator txCoordinator, RpcManager rpcManager,
                    RecoveryManager recoveryManager) {
      this.cacheConfiguration = c;
      this.txTable = txTable;
      this.txCoordinator = txCoordinator;
      this.rpcManager = rpcManager;
      this.recoveryManager = recoveryManager;
      this.statisticsEnabled = cacheConfiguration.jmxStatistics().enabled();
      this.isTotalOrder = c.transaction().transactionProtocol().isTotalOrder();
      useOnePhaseForAutoCommitTx = cacheConfiguration.transaction().use1PcForAutoCommitTransactions();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if it is remote and 2PC then first log the tx only after replying mods
      if (this.statisticsEnabled) prepares.incrementAndGet();
      Object result = invokeNextInterceptorAndVerifyTransaction(ctx, command);
      if (!ctx.isOriginLocal()) {
         if (command.isOnePhaseCommit()) {
            txTable.remoteTransactionCommitted(command.getGlobalTransaction(), true);
         } else {
            txTable.remoteTransactionPrepared(command.getGlobalTransaction());
         }
      }
      return result;
   }

   private Object invokeNextInterceptorAndVerifyTransaction(TxInvocationContext ctx, AbstractTransactionBoundaryCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (!ctx.isOriginLocal()) {
            //It is possible to receive a prepare or lock control command from a node that crashed. If that's the case rollback
            //the transaction forcefully in order to cleanup resources.
            boolean originatorMissing = !rpcManager.getTransport().getMembers().contains(command.getOrigin());
            boolean alreadyCompleted = txTable.isTransactionCompleted(command.getGlobalTransaction());
            if (log.isTraceEnabled()) {
               log.tracef("invokeNextInterceptorAndVerifyTransaction :: originatorMissing=%s, alreadyCompleted=%s",
                          originatorMissing, alreadyCompleted);
            }

            if (alreadyCompleted || originatorMissing) {
               if (log.isTraceEnabled()) {
                  log.tracef("Rolling back remote transaction %s because either already completed(%s) or originator no " +
                                   "longer in the cluster(%s).",
                             command.getGlobalTransaction(), alreadyCompleted, originatorMissing);
               }
               RollbackCommand rollback = new RollbackCommand(command.getCacheName(), command.getGlobalTransaction());
               try {
                  invokeNextInterceptor(ctx, rollback);
               } finally {
                  RemoteTransaction remoteTx = (RemoteTransaction) ctx.getCacheTransaction();
                  remoteTx.markForRollback(true);
                  txTable.removeRemoteTransaction(command.getGlobalTransaction());
               }
            }
         }
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GlobalTransaction gtx = ctx.getGlobalTransaction();
      // TODO The local origin check is needed for CommitFailsTest, but it doesn't appear correct to roll back an in-doubt tx
      if (!ctx.isOriginLocal()) {
         if (txTable.isTransactionCompleted(gtx)) {
            log.tracef("Transaction %s already completed, skipping commit", gtx);
            return null;
         } else {
            txTable.markTransactionCompleted(gtx);
         }
      }

      if (this.statisticsEnabled) commits.incrementAndGet();
      Object result = invokeNextInterceptor(ctx, command);
      if (!ctx.isOriginLocal() || isTotalOrder) {
         txTable.remoteTransactionCommitted(gtx, false);
      }
      return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (this.statisticsEnabled) rollbacks.incrementAndGet();
      // The transaction was marked as completed in RollbackCommand.prepare()
      if (!ctx.isOriginLocal() || isTotalOrder) {
         txTable.remoteTransactionRollback(command.getGlobalTransaction());
      }
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         //for tx that rollback we do not send a TxCompletionNotification, so we should cleanup
         // the recovery info here
         if (recoveryManager!=null) {
            GlobalTransaction gtx = command.getGlobalTransaction();
            recoveryManager.removeRecoveryInformation(((RecoverableTransactionIdentifier)gtx).getXid());
         }
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      enlistIfNeeded(ctx);

      if (ctx.isOriginLocal()) {
         command.setGlobalTransaction(ctx.getGlobalTransaction());
      }

      return invokeNextInterceptorAndVerifyTransaction(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, invalidateCommand);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   private Object enlistReadAndInvokeNext(InvocationContext ctx, VisitableCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNextInterceptor(ctx, command);
   }

   private void enlistIfNeeded(InvocationContext ctx) throws SystemException {
      if (shouldEnlist(ctx)) {
         enlist((TxInvocationContext) ctx);
      }
   }

   private Object enlistWriteAndInvokeNext(InvocationContext ctx, WriteCommand command) throws Throwable {
      LocalTransaction localTransaction = null;
      if (shouldEnlist(ctx)) {
         localTransaction = enlist((TxInvocationContext) ctx);
         boolean implicitWith1Pc = useOnePhaseForAutoCommitTx && localTransaction.isImplicitTransaction();
         if (implicitWith1Pc) {
            //in this situation we don't support concurrent updates so skip locking entirely
            command.setFlags(Flag.SKIP_LOCKING);
         }
      }
      Object rv;
      try {
         rv = invokeNextInterceptor(ctx, command);
      } catch (Throwable throwable) {
         // Don't mark the transaction for rollback if it's fail silent (i.e. putForExternalRead)
         if (ctx.isOriginLocal() && ctx.isInTxScope() && !command.hasFlag(Flag.FAIL_SILENTLY)) {
            TxInvocationContext txCtx = (TxInvocationContext) ctx;
            txCtx.getTransaction().setRollbackOnly();
         }
         throw throwable;
      }
      if (localTransaction != null && command.isSuccessful()) {
         localTransaction.addModification(command);
      }
      return rv;
   }

   public LocalTransaction enlist(TxInvocationContext ctx) throws SystemException {
      Transaction transaction = ctx.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      int status = transaction.getStatus();
      if (isNotValid(status)) throw new IllegalStateException("Transaction " + transaction +
            " is not in a valid state to be invoking cache operations on.");
      LocalTransaction localTransaction = txTable.getLocalTransaction(transaction);
      txTable.enlist(transaction, localTransaction);
      return localTransaction;
   }

   private boolean isNotValid(int status) {
      return status != Status.STATUS_ACTIVE
            && status != Status.STATUS_PREPARING
            && status != Status.STATUS_COMMITTING;
   }

   private static boolean shouldEnlist(InvocationContext ctx) {
      return ctx.isInTxScope() && ctx.isOriginLocal();
   }

   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset Statistics"
   )
   public void resetStatistics() {
      prepares.set(0);
      commits.set(0);
      rollbacks.set(0);
   }

   @ManagedAttribute(
         displayName = "Statistics enabled",
         dataType = DataType.TRAIT,
         writable = true
   )
   public boolean isStatisticsEnabled() {
      return this.statisticsEnabled;
   }

   @ManagedAttribute(
         description = "Number of transaction prepares performed since last reset",
         displayName = "Prepares",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getPrepares() {
      return prepares.get();
   }

   @ManagedAttribute(
         description = "Number of transaction commits performed since last reset",
         displayName = "Commits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getCommits() {
      return commits.get();
   }

   @ManagedAttribute(
         description = "Number of transaction rollbacks performed since last reset",
         displayName = "Rollbacks",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRollbacks() {
      return rollbacks.get();
   }
}
