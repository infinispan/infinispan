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


import org.infinispan.commands.LockControlCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
 *
 * @author Bela Ban
 * @since 4.0
 */
public class CallInterceptor extends CommandInterceptor {
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handlePrepareCommand.");
      return null;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleCommitCommand.");
      return null;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleRollbackCommand.");
      return null;
   }
   
   @Override
   public Object visitLockControlCommand(InvocationContext ctx, LockControlCommand c) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleLockControlCommand.");
      return null;
   }

   @Override
   final public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      Object retval;
      try {
         retval = command.perform(ctx);
      }
      catch (Throwable t) {
         if (ctx.isInTxScope()) {
            TxInvocationContext txContext = (TxInvocationContext) ctx;
            if (txContext.isValidRunningTx()) {
               txContext.getRunningTransaction().setRollbackOnly();
            }
         }
         throw t;
      }
      return retval;
   }
}