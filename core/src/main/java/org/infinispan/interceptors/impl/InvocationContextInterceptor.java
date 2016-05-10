package org.infinispan.interceptors.impl;


import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.BaseSequentialInterceptor;
import org.infinispan.interceptors.totalorder.RetryPrepareException;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 9.0
 */
public class InvocationContextInterceptor extends BaseSequentialInterceptor {

   private TransactionManager tm;
   private ComponentRegistry componentRegistry;
   private TransactionTable txTable;
   private InvocationContextContainer invocationContextContainer;

   private static final Log log = LogFactory.getLog(InvocationContextInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private volatile boolean shuttingDown = false;

   private final ReturnHandler defaultReturnHandler = new ReturnHandler() {
      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         invocationContextContainer.clearThreadLocal(rCtx);
         if (throwable == null)
            return null;

         if (throwable instanceof InvalidCacheUsageException) {
            throw throwable;
         } else {
            rethrowException(rCtx, rCommand, throwable);
         }
         if (rCommand instanceof LockControlCommand) {
            // Return null to return the same value without allocating a CompletableFuture
            return rv != null ? null : CompletableFuture.completedFuture(Boolean.FALSE);
         } else {
            // To ignore the exception, we need to return a CompletableFuture
            return CompletableFuture.completedFuture(rv);
         }
      }
   };

   @Start(priority = 1)
   private void setStartStatus() {
      shuttingDown = false;
   }

   @Stop(priority = 1)
   private void setStopStatus() {
      shuttingDown = true;
   }

   @Inject
   public void init(TransactionManager tm, ComponentRegistry componentRegistry, TransactionTable txTable, InvocationContextContainer invocationContextContainer) {
      this.tm = tm;
      this.componentRegistry = componentRegistry;
      this.txTable = txTable;
      this.invocationContextContainer = invocationContextContainer;
   }


   @Override
   public CompletableFuture<Void> visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (trace)
         log.tracef("Invoked with command %s and InvocationContext [%s]", command, ctx);
      if (ctx == null)
         throw new IllegalStateException("Null context not allowed!!");

      ComponentStatus status = componentRegistry.getStatus();
      if (command.ignoreCommandOnStatus(status)) {
         log.debugf("Status: %s : Ignoring %s command", status, command);
            return ctx.shortCircuit(null);
      } else {
         if (status.isTerminated()) {
            throw log.cacheIsTerminated(getCacheNamePrefix());
         } else if (stoppingAndNotAllowed(status, ctx)) {
            throw log.cacheIsStopping(getCacheNamePrefix());
         }
      }

      invocationContextContainer.setThreadLocal(ctx);
      return ctx.onReturn(defaultReturnHandler);
   }

   private void rethrowException(InvocationContext ctx, VisitableCommand command, Throwable th)
         throws Throwable {
      // Only check for fail silently if there's a failure :)
      boolean suppressExceptions = (command instanceof FlagAffectedCommand)
            && ((FlagAffectedCommand) command).hasFlag(Flag.FAIL_SILENTLY);
      // If we are shutting down there is every possibility that the invocation fails.
      suppressExceptions = suppressExceptions || shuttingDown;
      if (suppressExceptions) {
         if (shuttingDown)
            log.trace("Exception while executing code, but we're shutting down so failing silently.", th);
         else
            log.trace("Exception while executing code, failing silently...", th);
      } else {
         if (th instanceof WriteSkewException) {
            // We log this as DEBUG rather than ERROR - see ISPN-2076
            log.debug("Exception executing call", th);
         } else if (th instanceof OutdatedTopologyException) {
            log.outdatedTopology(th);
         } else if (th instanceof RetryPrepareException) {
            log.debugf("Retrying total order prepare command for transaction %s, affected keys %s",
                  ctx.getLockOwner(), toStr(extractWrittenKeys(ctx, command)));
         } else {
            Collection<Object> affectedKeys = extractWrittenKeys(ctx, command);
            log.executionError(command.getClass().getSimpleName(), toStr(affectedKeys), th);
         }
         if (ctx.isInTxScope() && ctx.isOriginLocal()) {
            if (trace) log.trace("Transaction marked for rollback as exception was received.");
            markTxForRollbackAndRethrow(ctx, th);
            throw new IllegalStateException("This should not be reached");
         }
         throw th;
      }
   }

   private Collection<Object> extractWrittenKeys(InvocationContext ctx, VisitableCommand command) {
      if (command instanceof WriteCommand) {
         return ((WriteCommand) command).getAffectedKeys();
      } else if (command instanceof LockControlCommand) {
         return Collections.emptyList();
      } else if (command instanceof TransactionBoundaryCommand) {
         return ((TxInvocationContext<AbstractCacheTransaction>) ctx).getAffectedKeys();
      }
      return Collections.emptyList();
   }

   private String getCacheNamePrefix() {
      String cacheName = componentRegistry.getCacheName();
      String prefix = "Cache '" + cacheName + "'";
      if (cacheName.equals(CacheContainer.DEFAULT_CACHE_NAME))
         prefix = "Default cache";
      return prefix;
   }

   /**
    * If the cache is STOPPING, non-transaction invocations, or transactional invocations for transaction others than
    * the ongoing ones, are no allowed. This method returns true if under this circumstances meet. Otherwise, it returns
    * false.
    */
   private boolean stoppingAndNotAllowed(ComponentStatus status, InvocationContext ctx) throws Exception {
      return status.isStopping() && (!ctx.isInTxScope() || !isOngoingTransaction(ctx));
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

   private boolean isValidRunningTx(Transaction tx) throws Exception {
      int status;
      try {
         status = tx.getStatus();
      } catch (SystemException e) {
         throw new CacheException("Unexpected!", e);
      }
      return status == Status.STATUS_ACTIVE;
   }

   private boolean isOngoingTransaction(InvocationContext ctx) throws SystemException {
      if (!ctx.isInTxScope())
         return false;

      if (ctx.isOriginLocal())
         return txTable.containsLocalTx(tm.getTransaction());
      else
         return txTable.containRemoteTx(((TxInvocationContext) ctx).getGlobalTransaction());
   }
}
