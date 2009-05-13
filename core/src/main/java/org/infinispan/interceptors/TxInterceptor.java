package org.infinispan.interceptors;

import org.infinispan.commands.LockControlCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.TransactionTable;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Mircea: Document this!
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TxInterceptor extends CommandInterceptor {

   private TransactionManager tm;
   private TransactionLog transactionLog;
   private TransactionTable txTable;

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);
   private boolean statsEnabled;


   @Inject
   public void init(TransactionManager tm, TransactionTable txTable, TransactionLog transactionLog) {
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
      if (!command.isOnePhaseCommit()) {
         transactionLog.logPrepare(command);
      } else {
         transactionLog.logOnePhaseCommit(ctx.getGlobalTransaction(), Arrays.asList(command.getModifications()));
      }
      if (getStatisticsEnabled()) prepares.incrementAndGet();
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (getStatisticsEnabled()) commits.incrementAndGet();
      transactionLog.logCommit(command.getGlobalTransaction());
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (getStatisticsEnabled()) rollbacks.incrementAndGet();
      transactionLog.rollback(command.getGlobalTransaction());
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(InvocationContext ctx, LockControlCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
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
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, invalidateCommand);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
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
      if (shouldEnlist(ctx)) {
         TransactionXaAdapter xaAdapter = enlist(ctx);
         LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
         if (!isLocalModeForced(ctx)) {
            xaAdapter.addModification(command);
         }
         localTxContext.setXaCache(xaAdapter);
      }
      if (!ctx.isInTxScope())
         transactionLog.logNoTxWrite(command);
      return invokeNextInterceptor(ctx, command);
   }

   public TransactionXaAdapter enlist(InvocationContext ctx) throws SystemException, RollbackException {
      Transaction transaction = tm.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      int status = transaction.getStatus();
      if (!isValid(status)) throw new IllegalStateException("Transaction " + transaction +
            " is not in a valid state to be invoking cache operations on.");
      return txTable.getOrCreateXaAdapter(transaction, ctx);
   }

   private boolean isValid(int status) {
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }

   private boolean shouldEnlist(InvocationContext ctx) {
      return ctx.isInTxScope() & ctx.isOriginLocal();
   }

   private boolean isLocalModeForced(InvocationContext icx) {
      if (icx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (log.isDebugEnabled()) log.debug("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }

   @ManagedOperation
   public void resetStatistics() {
      prepares.set(0);
      commits.set(0);
      rollbacks.set(0);
   }

   @ManagedAttribute
   public boolean getStatisticsEnabled() {
      return this.statsEnabled;
   }

   @ManagedAttribute
   public void setStatisticsEnabled(boolean enabled) {
      this.statsEnabled = enabled;
   }

   @ManagedAttribute(description = "number of transaction prepares")
   public long getPrepares() {
      return prepares.get();
   }

   @ManagedAttribute(description = "number of transaction commits")
   public long getCommits() {
      return commits.get();
   }

   @ManagedAttribute(description = "number of transaction rollbacks")
   public long getRollbacks() {
      return rollbacks.get();
   }
}
