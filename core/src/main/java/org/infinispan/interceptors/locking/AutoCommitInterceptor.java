/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.interceptors.locking;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

import javax.transaction.TransactionManager;

/**
 * If the cache is transactional and transactionAutoCommit is set to true this interceptor is added to the chain in
 * order to inject transactions when needed.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class AutoCommitInterceptor extends CommandInterceptor {

   private TransactionManager transactionManager;
   private InvocationContextContainer icc;


   @Inject
   public void init(TransactionManager tm, InvocationContextContainer icc) {
      this.transactionManager = tm;
      this.icc = icc;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return injectTransactionIfNeeded(ctx, command);
   }

   private Object injectTransactionIfNeeded(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (!ctx.isInTxScope()) {
         if (ctx.isUseFutureReturnType())
            throw new IllegalStateException("Future calls cannot run in auto-commit mode.");
         transactionManager.begin();
         InvocationContext txContext = icc.createInvocationContext();
         txContext.setClassLoader(ctx.getClassLoader());
         txContext.setFlags(ctx.getFlags());
         try {
            final Object result = invokeNextInterceptor(txContext, command);
            transactionManager.commit();
            return result;
         } catch (Throwable t) {
            log.couldNotCompleteInjectedTransaction(t);
            tryToRollback();
            throw t;
         }
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   private void tryToRollback() {
      try {
         transactionManager.rollback();
      } catch (Throwable e) {//this is best effort
         log.trace("Could not rollback transaction", e);
      }
   }
}
