/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
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
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
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

   private TransactionLog transactionLog;
   private TransactionTable txTable;

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);
   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   private boolean statisticsEnabled;
   protected TransactionCoordinator txCoordinator;

   private static final Log log = LogFactory.getLog(TxInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(TransactionTable txTable, TransactionLog transactionLog, Configuration c, TransactionCoordinator txCoordinator) {
      this.configuration = c;
      this.transactionLog = transactionLog;
      this.txTable = txTable;
      this.txCoordinator = txCoordinator;
      setStatisticsEnabled(configuration.isExposeJmxStatistics());
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if it is remote and 2PC then first log the tx only after replying mods
      if (!command.isOnePhaseCommit()) {
         transactionLog.logPrepare(command);
      }
      if (this.statisticsEnabled) prepares.incrementAndGet();
      Object result = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) {
         transactionLog.logOnePhaseCommit(ctx.getGlobalTransaction(), command.getModifications());
      }
      if (!ctx.isOriginLocal()) {
         if (command.isOnePhaseCommit()) {
            txTable.remoteTransactionCommitted(command.getGlobalTransaction());
         } else {
            txTable.remoteTransactionPrepared(command.getGlobalTransaction());
         }
      }
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (this.statisticsEnabled) commits.incrementAndGet();
      Object result = invokeNextInterceptor(ctx, command);
      if (!ctx.isOriginLocal()) {
         txTable.remoteTransactionCommitted(ctx.getGlobalTransaction());
      }
      transactionLog.logCommit(command.getGlobalTransaction());
      return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (this.statisticsEnabled) rollbacks.incrementAndGet();
      transactionLog.rollback(command.getGlobalTransaction());
      if (!ctx.isOriginLocal()) {
         txTable.remoteTransactionRollback(command.getGlobalTransaction());
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      enlistIfNeeded(ctx);

      if (ctx.isOriginLocal()) {
         command.setGlobalTransaction(ctx.getGlobalTransaction());
      }

      return invokeNextInterceptor(ctx, command);
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
         LocalTransaction localTransaction = enlist((TxInvocationContext) ctx);
         LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
         localTxContext.setLocalTransaction(localTransaction);
      }
   }

   private Object enlistWriteAndInvokeNext(InvocationContext ctx, WriteCommand command) throws Throwable {
      LocalTransaction localTransaction = null;
      boolean shouldAddMod = false;
      if (shouldEnlist(ctx)) {
         localTransaction = enlist((TxInvocationContext) ctx);
         LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
         if (localModeNotForced(ctx)) shouldAddMod = true;
         localTxContext.setLocalTransaction(localTransaction);
      }
      Object rv;
      try {
         rv = invokeNextInterceptor(ctx, command);
      } catch (Throwable throwable) {
         // Don't mark the transaction for rollback if it's fail silent (i.e. putForExternalRead)
         if (ctx.isOriginLocal() && ctx.isInTxScope() && !ctx.hasFlag(Flag.FAIL_SILENTLY)) {
            TxInvocationContext txCtx = (TxInvocationContext) ctx;
            txCtx.getTransaction().setRollbackOnly();
            final LocalTransaction cacheTransaction = (LocalTransaction) txCtx.getCacheTransaction();
            txTable.removeLocalTransaction(cacheTransaction);
         }
         throw throwable;
      }
      if (!ctx.isInTxScope())
         transactionLog.logNoTxWrite(command);
      if (command.isSuccessful() && shouldAddMod) localTransaction.addModification(command);
      return rv;
   }

   public LocalTransaction enlist(TxInvocationContext ctx) throws SystemException {
      Transaction transaction = ctx.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      int status = transaction.getStatus();
      if (isNotValid(status)) throw new IllegalStateException("Transaction " + transaction +
            " is not in a valid state to be invoking cache operations on.");
      LocalTransaction localTransaction = txTable.getOrCreateLocalTransaction(transaction, ctx);
      txTable.enlist(transaction, localTransaction);
      return localTransaction;
   }

   private boolean isNotValid(int status) {
      return status != Status.STATUS_ACTIVE && status != Status.STATUS_PREPARING;
   }

   private static boolean shouldEnlist(InvocationContext ctx) {
      return ctx.isInTxScope() && ctx.isOriginLocal();
   }

   private boolean localModeNotForced(InvocationContext icx) {
      if (icx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (getLog().isTraceEnabled()) getLog().debug("LOCAL mode forced on invocation.  Suppressing clustered events.");
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
}
