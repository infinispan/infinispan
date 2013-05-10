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
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
 *
 * @author Bela Ban
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class CallInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(CallInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private CacheNotifier notifier;

   @Inject
   public void inject(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   protected Log getLog() {
      return log;
   }

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
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand c) throws Throwable {
      if (trace) log.trace("Suppressing invocation of method handleLockControlCommand.");
      return null;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      Object ret = command.perform(ctx);
      if (ret != null) {
         if (command.isReturnEntry())
            notifyCacheEntryVisit(ctx, command, ((CacheEntry) ret).getValue());
         else
            notifyCacheEntryVisit(ctx, command, ret);
      }

      return ret;
   }

   private void notifyCacheEntryVisit(InvocationContext ctx, GetKeyValueCommand command, Object value) {
      Object key = command.getKey();
      notifier.notifyCacheEntryVisited(key, value, true, ctx, command);
      notifier.notifyCacheEntryVisited(key, value, false, ctx, command);
   }

   @Override
   final public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (trace) log.trace("Executing command: " + command + ".");
      return command.perform(ctx);
   }
}