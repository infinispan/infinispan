package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.atomic.AtomicLong;

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

   private TransactionManager tm;
   private TransactionLog transactionLog;
   private TransactionTable txTable;

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);
   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   private boolean statisticsEnabled;


   @Inject
   public void init(TransactionManager tm, TransactionTable txTable, TransactionLog transactionLog, Configuration c) {
      this.configuration = c;
      this.tm = tm;
      this.transactionLog = transactionLog;
      this.txTable = txTable;
      setStatisticsEnabled(configuration.isExposeJmxStatistics());
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         // replay modifications
         for (VisitableCommand modification : command.getModifications()) {
            VisitableCommand toReplay = getCommandToReplay(modification);
            if (toReplay != null) {
               invokeNextInterceptor(ctx, toReplay);
            }
         }
      }
      //if it is remote and 2PC then first log the tx only after replying mods
      if (!command.isOnePhaseCommit()) {
         transactionLog.logPrepare(command);
      }
      if (this.statisticsEnabled) prepares.incrementAndGet();
      Object result = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) {
         transactionLog.logOnePhaseCommit(ctx.getGlobalTransaction(), command.getModifications());
      }
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (this.statisticsEnabled) commits.incrementAndGet();
      Object result = invokeNextInterceptor(ctx, command);
      transactionLog.logCommit(command.getGlobalTransaction());
      return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (this.statisticsEnabled) rollbacks.incrementAndGet();
      transactionLog.rollback(command.getGlobalTransaction());
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      try {
         return enlistReadAndInvokeNext(ctx, command);
      } catch (Throwable t) {
         return markTxForRollbackAndRethrow(ctx, t);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
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
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, invalidateCommand);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   private Object enlistReadAndInvokeNext(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (shouldEnlist(ctx)) {
         TransactionXaAdapter xaAdapter = enlist(ctx);
         LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
         localTxContext.setXaCache(xaAdapter);
      }
      return invokeNextInterceptor(ctx, command);
   }

   private Object enlistWriteAndInvokeNext(InvocationContext ctx, WriteCommand command) throws Throwable {
      TransactionXaAdapter xaAdapter = null;
      boolean shouldAddMod = false;
      if (shouldEnlist(ctx)) {
         xaAdapter = enlist(ctx);
         LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
         if (localModeNotForced(ctx)) shouldAddMod = true;
         localTxContext.setXaCache(xaAdapter);
      }
      Object rv;
      rv = invokeNextAndRollbackTxOnFailure(ctx, command);
      if (!ctx.isInTxScope())
         transactionLog.logNoTxWrite(command);
      if (command.isSuccessful() && shouldAddMod) xaAdapter.addModification(command);
      return rv;
   }

   public TransactionXaAdapter enlist(InvocationContext ctx) throws SystemException, RollbackException {
      Transaction transaction = tm.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      int status = transaction.getStatus();
      if (isNotValid(status)) throw new IllegalStateException("Transaction " + transaction +
            " is not in a valid state to be invoking cache operations on.");
      return txTable.getOrCreateXaAdapter(transaction, ctx);
   }

   private boolean isNotValid(int status) {
      return status != Status.STATUS_ACTIVE && status != Status.STATUS_PREPARING;
   }

   private boolean shouldEnlist(InvocationContext ctx) {
      return ctx.isInTxScope() && ctx.isOriginLocal();
   }

   private boolean localModeNotForced(InvocationContext icx) {
      if (icx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) log.debug("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return false;
      }
      return true;
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics")
   public void resetStatistics() {
      prepares.set(0);
      commits.set(0);
      rollbacks.set(0);
   }

   @Operation(displayName = "Enable/disable statistics")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean enabled) {
      this.statisticsEnabled = enabled;
   }

   @Metric(displayName = "Statistics enabled", dataType = DataType.TRAIT)
   public boolean isStatisticsEnabled() {
      return this.statisticsEnabled;
   }

   @ManagedAttribute(description = "Number of transaction prepares performed since last reset")
   @Metric(displayName = "Prepares", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getPrepares() {
      return prepares.get();
   }

   @ManagedAttribute(description = "Number of transaction commits performed since last reset")
   @Metric(displayName = "Commits", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getCommits() {
      return commits.get();
   }

   @ManagedAttribute(description = "Number of transaction rollbacks performed since last reset")
   @Metric(displayName = "Rollbacks", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getRollbacks() {
      return rollbacks.get();
   }

   /**
    * Designed to be overridden.  Returns a VisitableCommand fit for replaying locally, based on the modification passed
    * in.  If a null value is returned, this means that the command should not be replayed.
    *
    * @param modification modification in a prepare call
    * @return a VisitableCommand representing this modification, fit for replaying, or null if the command should not be
    *         replayed.
    */
   protected VisitableCommand getCommandToReplay(VisitableCommand modification) {
      return modification;
   }

   private Object markTxForRollbackAndRethrow(InvocationContext ctx, Throwable te) throws Throwable {
      if (ctx.isOriginLocal() && ctx.isInTxScope()) {
         Transaction transaction = tm.getTransaction();
         if (transaction != null && isValidRunningTx(transaction)) {
            transaction.setRollbackOnly();
         }
      }
      throw te;
   }

   private Object invokeNextAndRollbackTxOnFailure(InvocationContext ctx, WriteCommand command) throws Throwable {
      Object rv;
      try {
         rv = invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         markTxForRollbackAndRethrow(ctx, te);
         throw new IllegalStateException("This should not be reached");
      }
      return rv;
   }

   public boolean isValidRunningTx(Transaction tx) throws Exception {
      int status;
      try {
         status = tx.getStatus();
      }
      catch (SystemException e) {
         throw new CacheException("Unexpected!", e);
      }
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }
}
