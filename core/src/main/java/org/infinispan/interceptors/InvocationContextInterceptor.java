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


import org.infinispan.CacheException;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.statetransfer.StateTransferInProgressException;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class InvocationContextInterceptor extends CommandInterceptor {

   private TransactionManager tm;
   private ComponentRegistry componentRegistry;
   private TransactionTable txTable;
   private InvocationContextContainer invocationContextContainer;

   private static final Log log = LogFactory.getLog(InvocationContextInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private volatile boolean shuttingDown = false;

   @Override
   protected Log getLog() {
      return log;
   }

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
   public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return handleAll(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand lcc) throws Throwable {
      Object retval = handleAll(ctx, lcc);
      return retval == null ? false : retval;
   }

   private Object handleAll(InvocationContext ctx, VisitableCommand command) throws Throwable {
      boolean suppressExceptions = false;
      try {
         ComponentStatus status = componentRegistry.getStatus();
         if (command.ignoreCommandOnStatus(status)) {
            log.debugf("Status: %s : Ignoring %s command", status, command);
            return null;
         }

         if (status.isTerminated()) {
            throw new IllegalStateException(String.format(
                  "%s is in 'TERMINATED' state and so it does not accept new invocations. " +
                        "Either restart it or recreate the cache container.",
                  getCacheNamePrefix()));
         } else if (stoppingAndNotAllowed(status, ctx)) {
            throw new IllegalStateException(String.format(
                  "%s is in 'STOPPING' state and this is an invocation not belonging to an on-going transaction, so it does not accept new invocations. " +
                        "Either restart it or recreate the cache container.",
                  getCacheNamePrefix()));
         }

         LogFactory.pushNDC(componentRegistry.getCacheName(), trace);

         try {
            if (trace) log.tracef("Invoked with command %s and InvocationContext [%s]", command, ctx);
            if (ctx == null) throw new IllegalStateException("Null context not allowed!!");

            if (ctx.hasFlag(Flag.FAIL_SILENTLY)) {
               suppressExceptions = true;
            }

            try {
               return invokeNextInterceptor(ctx, command);
            } catch (Throwable th) {
               // If we are shutting down there is every possibility that the invocation fails.
               suppressExceptions = suppressExceptions || shuttingDown;
               if (suppressExceptions) {
                  if (shuttingDown)
                     log.trace("Exception while executing code, but we're shutting down so failing silently.");
                  else
                     log.trace("Exception while executing code, failing silently...", th);
                  return null;
               } else {
                  // HACK: There are way too many StateTransferInProgressExceptions on remote nodes during state transfer
                  // and they not really exceptional (the originator will retry the command)
                  boolean logAsError = !(th instanceof StateTransferInProgressException && !ctx.isOriginLocal());
                  if (logAsError) {
                     if (th instanceof WriteSkewException) {
                        // We log this as DEBUG rather than ERROR - see ISPN-2076
                        log.debug("Exception executing call", th);
                     } else {
                        log.executionError(th);
                     }
                  } else {
                     log.trace("Exception while executing code", th);
                  }
                  if (ctx.isInTxScope() && ctx.isOriginLocal()) {
                     if (trace) log.trace("Transaction marked for rollback as exception was received.");
                     markTxForRollbackAndRethrow(ctx, th);
                     throw new IllegalStateException("This should not be reached");
                  }
                  throw th;
               }
            } finally {
               ctx.reset();
            }
         } finally {
            LogFactory.popNDC(trace);
         }
      } finally {
         invocationContextContainer.clearThreadLocal();
      }
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
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }

   private boolean isOngoingTransaction(InvocationContext ctx) throws SystemException {
      return ctx.isInTxScope() && (txTable.containsLocalTx(tm.getTransaction()) || (!ctx.isOriginLocal() &&
                                                                                          txTable.containRemoteTx(((TxInvocationContext) ctx).getGlobalTransaction())));
   }
}
