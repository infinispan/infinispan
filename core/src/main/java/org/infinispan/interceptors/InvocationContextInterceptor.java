/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 */
public class InvocationContextInterceptor extends CommandInterceptor {

   private TransactionManager tm;

   @Inject
   public void init(TransactionManager tm) {
      this.tm = tm;
   }

   @Override
   public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return handleAll(ctx, command);
   }

   private Object handleAll(InvocationContext ctx, VisitableCommand command) throws Throwable {
      boolean suppressExceptions = false;

      if (trace) log.trace("Invoked with command " + command + " and InvocationContext [" + ctx + "]");
      if (ctx == null) throw new IllegalStateException("Null context not allowed!!");

      if (ctx.hasFlag(Flag.FAIL_SILENTLY)) {
         suppressExceptions = true;
      }

      try {
         return invokeNextInterceptor(ctx, command);
      }
      catch (Throwable th) {
         if (suppressExceptions) {
            log.trace("Exception while executing code, failing silently...", th);
            return null;
         } else {
            if (ctx.isInTxScope() && ctx.isOriginLocal()) {
               if (trace) log.trace("Transaction marked for rollback as exception was received.");
               markTxForRollbackAndRethrow(ctx, th);
               throw new IllegalStateException("This should not be reached");
            }
            log.error("Execution error: ", th);
            throw th;
         }
      } finally {
         ctx.reset();
      }
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