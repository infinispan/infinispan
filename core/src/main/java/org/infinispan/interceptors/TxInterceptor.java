package org.infinispan.interceptors;

import java.util.concurrent.atomic.AtomicLong;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
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
import org.infinispan.iteration.EntryIterable;
import org.infinispan.iteration.impl.TransactionAwareEntryIterable;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.LockingMode;
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
public class TxInterceptor extends CommandInterceptor implements JmxStatisticsExposer {

   private TransactionTable txTable;

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);
   private boolean statisticsEnabled;
   protected TransactionCoordinator txCoordinator;
   protected RpcManager rpcManager;

   private static final Log log = LogFactory.getLog(TxInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private RecoveryManager recoveryManager;
   private boolean isTotalOrder;
   private boolean useOnePhaseForAutoCommitTx;
   private boolean useVersioning;
   private CommandsFactory commandsFactory;
   private Cache cache;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(TransactionTable txTable, Configuration configuration, TransactionCoordinator txCoordinator, RpcManager rpcManager,
                    RecoveryManager recoveryManager, CommandsFactory commandsFactory, Cache cache) {
      this.cacheConfiguration = configuration;
      this.txTable = txTable;
      this.txCoordinator = txCoordinator;
      this.rpcManager = rpcManager;
      this.recoveryManager = recoveryManager;
      this.commandsFactory = commandsFactory;
      this.cache = cache;

      statisticsEnabled = cacheConfiguration.jmxStatistics().enabled();
      isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
      useOnePhaseForAutoCommitTx = cacheConfiguration.transaction().use1PcForAutoCommitTransactions();
      useVersioning = configuration.transaction().transactionMode().isTransactional() && configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC && configuration.versioning().enabled();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if it is remote and 2PC then first log the tx only after replying mods
      if (this.statisticsEnabled) prepares.incrementAndGet();
      if (!ctx.isOriginLocal()) {
         ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());
      } else {
         if (ctx.getCacheTransaction().hasModification(ClearCommand.class)) {
            throw new IllegalStateException("No ClearCommand is allowed in Transaction.");
         }
      }
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
            // command.getOrigin() and ctx.getOrigin() are not reliable for LockControlCommands started by
            // ClusteredGetCommands, or for PrepareCommands started by MultipleRpcCommands (when the replication queue
            // is enabled).
            Address origin = ctx.getGlobalTransaction().getAddress();
            //It is possible to receive a prepare or lock control command from a node that crashed. If that's the case rollback
            //the transaction forcefully in order to cleanup resources.
            boolean originatorMissing = !rpcManager.getTransport().getMembers().contains(origin);
            // It is also possible that the LCC timed out on the originator's end and this node has processed
            // a TxCompletionNotification.  So we need to check the presence of the remote transaction to
            // see if we need to clean up any acquired locks on our end.
            boolean alreadyCompleted = txTable.isTransactionCompleted(command.getGlobalTransaction()) ||
                    !txTable.containRemoteTx(command.getGlobalTransaction());
            // We want to throw an exception if the originator left the cluster and the transaction is not finished
            // and/or it was rolled back by TransactionTable.cleanupLeaverTransactions().
            // We don't want to throw an exception if the originator left the cluster but the transaction already
            // completed successfully. So far, this only seems possible when forcing the commit of an orphaned
            // transaction (with recovery enabled).
            boolean completedSuccessfully = alreadyCompleted && !ctx.getCacheTransaction().isMarkedForRollback();
            if (trace) {
               log.tracef("invokeNextInterceptorAndVerifyTransaction :: originatorMissing=%s, alreadyCompleted=%s",
                       originatorMissing, alreadyCompleted);
            }

            if (alreadyCompleted || originatorMissing) {
               if (trace) {
                  log.tracef("Rolling back remote transaction %s because either already completed (%s) or originator no " +
                          "longer in the cluster (%s).",
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

            if (originatorMissing && !completedSuccessfully) {
               throw log.orphanTransactionRolledBack(ctx.getGlobalTransaction());
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
            if (trace) log.tracef("Transaction %s already completed, skipping commit", gtx);
            return null;
         }

         if (!isTotalOrder) {
            // If a commit is received for a transaction that doesn't have its 'lookedUpEntries' populated
            // we know for sure this transaction is 2PC and was received via state transfer but the preceding PrepareCommand
            // was not received by local node because it was executed on the previous key owners. We need to re-prepare
            // the transaction on local node to ensure its locks are acquired and lookedUpEntries is properly populated.
            RemoteTransaction remoteTx = (RemoteTransaction) ctx.getCacheTransaction();
            if (trace) {
               log.tracef("Remote tx topology id %d and command topology is %d", remoteTx.lookedUpEntriesTopology(),
                          command.getTopologyId());
            }
            if (remoteTx.lookedUpEntriesTopology() < command.getTopologyId()) {
               PrepareCommand prepareCommand;
               if (useVersioning) {
                  prepareCommand = commandsFactory.buildVersionedPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
               } else {
                  prepareCommand = commandsFactory.buildPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
               }
               commandsFactory.initializeReplicableCommand(prepareCommand, true);
               prepareCommand.setOrigin(ctx.getOrigin());
               if (trace)
                  log.tracef("Replaying the transactions received as a result of state transfer %s", prepareCommand);
               visitPrepareCommand(ctx, prepareCommand);
            }
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
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
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

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }
   
   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   @Override
   public EntryIterable visitEntryRetrievalCommand(InvocationContext ctx, EntryRetrievalCommand command) throws Throwable {
      // Enlistment shouldn't be needed for this command.  The remove on the iterator will internally make a remove
      // command and the iterator itself does not place read values into the context.
      EntryIterable iterable = (EntryIterable) super.visitEntryRetrievalCommand(ctx, command);
      if (ctx.isInTxScope()) {
         return new TransactionAwareEntryIterable(iterable, command.getFilter(), 
               (TxInvocationContext<LocalTransaction>) ctx, cache);
      } else {
         return iterable;
      }
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
      } catch (OutdatedTopologyException e) {
         // The command will be retried, so we shouldn't mark the transaction for rollback
         throw e;
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

   @Override
   public boolean getStatisticsEnabled() {
      return isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
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
