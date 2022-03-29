package org.infinispan.interceptors.impl;

import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.concurrent.locks.LockReleasedException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with handling transaction related operations, e.g enlisting cache as an transaction
 * participant, propagating remotely initiated changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.transaction.xa.TransactionXaAdapter
 * @since 9.0
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class TxInterceptor<K, V> extends DDAsyncInterceptor implements JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(TxInterceptor.class);

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);

   @Inject CommandsFactory commandsFactory;
   @Inject ComponentRef<Cache<K, V>> cache;
   @Inject RecoveryManager recoveryManager;
   @Inject TransactionTable txTable;
   @Inject KeyPartitioner keyPartitioner;

   private boolean useOnePhaseForAutoCommitTx;
   private boolean useVersioning;
   private boolean statisticsEnabled;

   private static void checkTransactionThrowable(CacheTransaction tx, Throwable throwable) {
      if (tx.isMarkedForRollback() && throwable instanceof LockReleasedException) {
         throw log.transactionAlreadyRolledBack(tx.getGlobalTransaction());
      }
   }

   @Start
   public void start() {
      statisticsEnabled = cacheConfiguration.statistics().enabled();
      useOnePhaseForAutoCommitTx = cacheConfiguration.transaction().use1PcForAutoCommitTransactions();
      useVersioning = Configurations.isTxVersioned(cacheConfiguration);
   }

   @Override
   public Object visitPrepareCommand(@SuppressWarnings("rawtypes") TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handlePrepareCommand(ctx, command);
   }

   private Object handlePrepareCommand(TxInvocationContext<?> ctx, PrepareCommand command) {
      // Debugging for ISPN-5379
      ctx.getCacheTransaction().freezeModifications();

      //if it is remote and 2PC then first log the tx only after replying mods
      if (this.statisticsEnabled) prepares.incrementAndGet();
      if (!ctx.isOriginLocal()) {
         ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());
         Object verifyResult = invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            if (!rCtx.isOriginLocal()) {
               return verifyRemoteTransaction((RemoteTxInvocationContext) rCtx, rCommand, rv, throwable);
            }

            return valueOrException(rv, throwable);
         });
         return makeStage(verifyResult).thenAccept(ctx, command, (rCtx, prepareCommand, rv) -> {
            if (prepareCommand.isOnePhaseCommit()) {
               txTable.remoteTransactionCommitted(prepareCommand.getGlobalTransaction(), true);
            } else {
               txTable.remoteTransactionPrepared(prepareCommand.getGlobalTransaction());
            }
         });
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(@SuppressWarnings("rawtypes") TxInvocationContext ctx, CommitCommand command) throws Throwable {
      // TODO The local origin check is needed for CommitFailsTest, but it doesn't appear correct to roll back an in-doubt tx
      if (!ctx.isOriginLocal()) {
         GlobalTransaction gtx = ctx.getGlobalTransaction();
         if (txTable.isTransactionCompleted(gtx)) {
            if (log.isTraceEnabled()) log.tracef("Transaction %s already completed, skipping commit", gtx);
            return null;
         }

         InvocationStage replayStage = replayRemoteTransactionIfNeeded((RemoteTxInvocationContext) ctx,
               command.getTopologyId());
         if (replayStage != null) {
            return replayStage.andHandle(ctx, command, (rCtx, rCommand, rv, t) ->
                  finishCommit((TxInvocationContext<?>) rCtx, rCommand));
         } else {
            return finishCommit(ctx, command);
         }
      }

      return finishCommit(ctx, command);
   }

   private Object finishCommit(TxInvocationContext<?> ctx, VisitableCommand command) {
      GlobalTransaction gtx = ctx.getGlobalTransaction();
      if (this.statisticsEnabled) commits.incrementAndGet();
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         if (!rCtx.isOriginLocal()) {
            txTable.remoteTransactionCommitted(gtx, false);
         }
      });
   }

   @Override
   public Object visitRollbackCommand(@SuppressWarnings("rawtypes") TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (this.statisticsEnabled) rollbacks.incrementAndGet();
      // The transaction was marked as completed in RollbackCommand.prepare()
      if (!ctx.isOriginLocal()) {
         txTable.remoteTransactionRollback(command.getGlobalTransaction());
      }
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         //for tx that rollback we do not send a TxCompletionNotification, so we should cleanup
         // the recovery info here
         if (recoveryManager != null) {
            GlobalTransaction gtx = rCommand.getGlobalTransaction();
            recoveryManager.removeRecoveryInformation(gtx.getXid());
         }
      });
   }

   @Override
   public Object visitLockControlCommand(@SuppressWarnings("rawtypes") TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      enlistIfNeeded(ctx);

      if (ctx.isOriginLocal()) {
         command.setGlobalTransaction(ctx.getGlobalTransaction());
      }

      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
         if (!rCtx.isOriginLocal()) {
            return verifyRemoteTransaction((RemoteTxInvocationContext) rCtx, rCommand, rv, throwable);
         }
         checkTransactionThrowable(((TxInvocationContext<?>) rCtx).getCacheTransaction(), throwable);
         return valueOrException(rv, throwable);
      });
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) {
      // Remove expired is non transactional
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || !ctx.isInTxScope())
         return invokeNext(ctx, command);

      enlistIfNeeded(ctx);
      // If we have any entries looked up - even read, we can't allow size optimizations
      if (ctx.isInTxScope() && ctx.lookedUpEntriesCount() > 0) {
         command.addFlags(FlagBitSets.SKIP_SIZE_OPTIMIZATION);
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (ctx.isOriginLocal() && ctx.isInTxScope()) {
         enlistIfNeeded(ctx);
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (ctx.isOriginLocal() && ctx.isInTxScope()) {
         enlistIfNeeded(ctx);
      }
      return invokeNext(ctx, command);

   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand)
         throws Throwable {
      return handleWriteCommand(ctx, invalidateCommand);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   private void enlistIfNeeded(InvocationContext ctx) throws SystemException {
      if (shouldEnlist(ctx)) {
         assert ctx instanceof LocalTxInvocationContext;
         enlist((LocalTxInvocationContext) ctx);
      }
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (shouldEnlist(ctx)) {
         assert ctx instanceof LocalTxInvocationContext;
         LocalTransaction localTransaction = enlist((LocalTxInvocationContext) ctx);
         boolean implicitWith1Pc = useOnePhaseForAutoCommitTx && localTransaction.isImplicitTransaction();
         if (implicitWith1Pc) {
            //in this situation we don't support concurrent updates so skip locking entirely
            command.addFlags(FlagBitSets.SKIP_LOCKING);
         }
      }
      return invokeNextAndFinally(ctx, command, (rCtx, writeCommand, rv, t) -> {
         // We shouldn't mark the transaction for rollback if it's going to be retried
         if (t != null && !(t instanceof OutdatedTopologyException)) {
            // Don't mark the transaction for rollback if it's fail silent (i.e. putForExternalRead)
            if (rCtx.isOriginLocal() && rCtx.isInTxScope() && !writeCommand.hasAnyFlag(FlagBitSets.FAIL_SILENTLY)) {
               TxInvocationContext<?> txCtx = (TxInvocationContext<?>) rCtx;
               // avoid invoke setRollbackOnly() if the transaction is already rolled back
               checkTransactionThrowable(txCtx.getCacheTransaction(), t);
               txCtx.getTransaction().setRollbackOnly();
            }
         }
         if (t == null && shouldEnlist(rCtx) && writeCommand.isSuccessful()) {
            assert rCtx instanceof LocalTxInvocationContext;
            ((LocalTxInvocationContext) rCtx).getCacheTransaction().addModification(writeCommand);
         }
      });
   }

   private LocalTransaction enlist(LocalTxInvocationContext ctx) throws SystemException {
      Transaction transaction = ctx.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      LocalTransaction localTransaction = ctx.getCacheTransaction();
      if (localTransaction.isFromStateTransfer()) {
         return localTransaction;
      }
      int status = transaction.getStatus();
      if (isNotValid(status)) {
         if (!localTransaction.isEnlisted()) {
            // This transaction wouldn't be removed by TM.commit() or TM.rollback()
            txTable.removeLocalTransaction(localTransaction);
         }
         throw new IllegalStateException("Transaction " + transaction +
                                               " is not in a valid state to be invoking cache operations on.");
      }
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

   @Override
   public boolean getStatisticsEnabled() {
      return isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
   }

   @Override
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
         measurementType = MeasurementType.TRENDSUP
   )
   public long getPrepares() {
      return prepares.get();
   }

   @ManagedAttribute(
         description = "Number of transaction commits performed since last reset",
         displayName = "Commits",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCommits() {
      return commits.get();
   }

   @ManagedAttribute(
         description = "Number of transaction rollbacks performed since last reset",
         displayName = "Rollbacks",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getRollbacks() {
      return rollbacks.get();
   }

   private Object verifyRemoteTransaction(RemoteTxInvocationContext ctx, AbstractTransactionBoundaryCommand command,
                                          Object rv, Throwable throwable) throws Throwable {
      final GlobalTransaction globalTransaction = command.getGlobalTransaction();

      // It is also possible that the LCC timed out on the originator's end and this node has processed
      // a TxCompletionNotification.  So we need to check the presence of the remote transaction to
      // see if we need to clean up any acquired locks on our end.
      boolean alreadyCompleted = txTable.isTransactionCompleted(globalTransaction) || !txTable.containRemoteTx(globalTransaction);

      if (log.isTraceEnabled()) {
         log.tracef("Verifying transaction: alreadyCompleted=%s", alreadyCompleted);
      }

      if (alreadyCompleted) {
         if (log.isTraceEnabled()) {
            log.tracef("Rolling back remote transaction %s because it was already completed",
                       globalTransaction);
         }
         // The rollback command only marks the transaction as completed in invokeAsync()
         txTable.markTransactionCompleted(globalTransaction, false);
         RollbackCommand rollback = commandsFactory.buildRollbackCommand(command.getGlobalTransaction());
         return invokeNextAndFinally(ctx, rollback, (rCtx, rCommand, rv1, throwable1) -> {
            //noinspection unchecked
            RemoteTransaction remoteTx = ((TxInvocationContext<RemoteTransaction>) rCtx).getCacheTransaction();
            remoteTx.markForRollback(true);
            txTable.removeRemoteTransaction(globalTransaction);
         });
      }

      return valueOrException(rv, throwable);
   }

   private InvocationStage replayRemoteTransactionIfNeeded(RemoteTxInvocationContext ctx, int topologyId)
         throws Throwable {
      // If a commit is received for a transaction that doesn't have its 'lookedUpEntries' populated
      // we know for sure this transaction is 2PC and was received via state transfer but the preceding PrepareCommand
      // was not received by local node because it was executed on the previous key owners. We need to re-prepare
      // the transaction on local node to ensure its locks are acquired and lookedUpEntries is properly populated.
      RemoteTransaction remoteTx = ctx.getCacheTransaction();
      if (log.isTraceEnabled()) {
         log.tracef("Remote tx topology id %d and command topology is %d", remoteTx.lookedUpEntriesTopology(), topologyId);
      }
      if (remoteTx.lookedUpEntriesTopology() < topologyId) {
         PrepareCommand prepareCommand;
         if (useVersioning) {
            prepareCommand = commandsFactory.buildVersionedPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
         } else {
            prepareCommand = commandsFactory.buildPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
         }
         prepareCommand.markTransactionAsRemote(true);
         prepareCommand.setOrigin(ctx.getOrigin());
         if (log.isTraceEnabled()) {
            log.tracef("Replaying the transactions received as a result of state transfer %s",
                  prepareCommand);
         }
         return makeStage(handlePrepareCommand(ctx, prepareCommand));
      }
      return null;
   }
}
