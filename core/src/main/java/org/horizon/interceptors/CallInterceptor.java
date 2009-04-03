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
package org.horizon.interceptors;


import org.horizon.commands.ReplicableCommand;
import org.horizon.commands.VisitableCommand;
import org.horizon.commands.tx.CommitCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.tx.RollbackCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.WriteCommand;
import org.horizon.context.InvocationContext;
import org.horizon.interceptors.base.CommandInterceptor;
import org.horizon.transaction.GlobalTransaction;

import javax.transaction.Transaction;

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
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handlePrepareCommand.");
      return null;
   }

   @Override
   public Object visitCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleCommitCommand.");
      return null;
   }

   @Override
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleRollbackCommand.");
      return null;
   }

   @Override
   public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      return invokeCommand(ctx, command);
   }

   private Object invokeCommand(InvocationContext ctx, ReplicableCommand command) throws Throwable {
      Object retval;
      try {
         retval = command.perform(ctx);
      }
      catch (Throwable t) {
         Transaction tx = ctx.getTransaction();
         if (ctx.isValidTransaction()) {
            tx.setRollbackOnly();
         }
         throw t;
      }
      return retval;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleAlterCacheMethod(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleAlterCacheMethod(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleAlterCacheMethod(ctx, command);
   }

   private Object handleAlterCacheMethod(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      Object result = invokeCommand(ctx, command);
      if (ctx.isValidTransaction()) {
         GlobalTransaction gtx = ctx.getGlobalTransaction();
         if (gtx == null) {
            if (log.isDebugEnabled()) {
               log.debug("didn't find GlobalTransaction for " + ctx.getTransaction() + "; won't add modification to transaction list");
            }
         } else {
            ctx.getTransactionContext().addModification(command);
         }
      }
      return result;
   }
}