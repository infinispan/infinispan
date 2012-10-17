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
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor populates the {@link org.infinispan.transaction.xa.DldGlobalTransaction} with
 * appropriate information needed in order to accomplish deadlock detection. It MUST process populate data before the
 * replication takes place, so it will do all the tasks before calling {@link org.infinispan.interceptors.base.CommandInterceptor#invokeNextInterceptor(org.infinispan.context.InvocationContext,
 * org.infinispan.commands.VisitableCommand)}.
 * <p/>
 * Note: for local caches, deadlock detection dos NOT work for aggregate operations (clear, putAll).
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class DeadlockDetectingInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(DeadlockDetectingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   /**
    * Only does a sanity check.
    */
   @Start
   public void start() {
      if (!cacheConfiguration.deadlockDetection().enabled()) {
         throw new IllegalStateException("This interceptor should not be present in the chain as deadlock detection is not used!");
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      DldGlobalTransaction globalTransaction = (DldGlobalTransaction) ctx.getGlobalTransaction();
      if (ctx.isOriginLocal()) {
         globalTransaction.setRemoteLockIntention(command.getKeys());
         //in the case of DIST we need to propagate the list of keys. In all other situations in can be determined
         // based on the actual command
         if (cacheConfiguration.clustering().cacheMode().isDistributed()) {
            if (log.isTraceEnabled()) log.tracef("Locks as seen at origin are: %s", ctx.getLockedKeys());
            ((DldGlobalTransaction) ctx.getGlobalTransaction()).setLocksHeldAtOrigin(ctx.getLockedKeys());
         }
      }
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      DldGlobalTransaction globalTransaction = (DldGlobalTransaction) ctx.getGlobalTransaction();
      if (ctx.isOriginLocal()) {
         globalTransaction.setRemoteLockIntention(command.getAffectedKeys());
      }
      Object result = invokeNextInterceptor(ctx, command);
      if (ctx.isOriginLocal()) {
         globalTransaction.setRemoteLockIntention(InfinispanCollections.emptySet());
      }
      return result;
   }


   private Object handleDataCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }
}
