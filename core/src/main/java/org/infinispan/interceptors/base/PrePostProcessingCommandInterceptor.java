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
package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * This interceptor adds pre and post processing to each <tt>visitXXX()</tt> method.
 * <p/>
 * For each <tt>visitXXX()</tt> method invoked, it will first call {@link #doBeforeCall(InvocationContext,
 * VisitableCommand)} and if this method returns true, it will proceed to invoking a <tt>handleXXX()</tt> method and
 * lastly, {@link #doAfterCall(InvocationContext, VisitableCommand)} in a <tt>finally</tt> block.  Note that the
 * <tt>doAfterCall()</tt> method is still invoked even if <tt>doBeforeCall()</tt> returns <tt>false</tt>.
 * <p/>
 * Instead of overriding <tt>visitXXX()</tt> methods, implementations should override their <tt>handleXXX()</tt>
 * counterparts defined in this class instead, as well as the {@link #doAfterCall(InvocationContext ,VisitableCommand)}
 * method and optionally {@link #doBeforeCall(InvocationContext, VisitableCommand)}.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class PrePostProcessingCommandInterceptor extends CommandInterceptor {
   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handlePutKeyValueCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handlePutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleRemoveCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleReplaceCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleClearCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handlePutMapCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handlePutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleEvictCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   // read commands

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleSizeCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleGetKeyValueCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   // tx commands

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handlePrepareCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handlePrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleRollbackCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return (doBeforeCall(ctx, command)) ? handleCommitCommand(ctx, command) : null;
      }
      finally {
         doAfterCall(ctx, command);
      }
   }

   protected Object handleCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   /**
    * Callback that is invoked after every handleXXX() method defined above.
    *
    * @param ctx     invocation context
    * @param command command which was invoked
    */
   protected abstract void doAfterCall(InvocationContext ctx, VisitableCommand command);

   protected boolean doBeforeCall(InvocationContext ctx, VisitableCommand command) {
      return true;
   }
}