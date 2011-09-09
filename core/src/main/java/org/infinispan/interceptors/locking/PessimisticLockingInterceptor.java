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

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;

import java.util.Collections;
import java.util.Set;

/**
 * Locking interceptor to be used by pessimistic caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class PessimisticLockingInterceptor extends AbstractTxLockingInterceptor {

   private CommandsFactory cf;

   @Inject
   public void init(CommandsFactory factory) {
      this.cf = factory;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         abortIfRemoteTransactionInvalid(ctx, command);
         return invokeNextAndCommitIf1Pc(ctx, command);
      } catch (Throwable t) {
         lockManager.releaseLocks(ctx);
         throw t;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         assertNoRemoteContext(ctx);
         acquireRemoteIfNeeded(ctx, Collections.singleton(command.getKey()));
         if (cll.localNodeIsOwner(command.getKey())) {
            lockKey(ctx, command.getKey());
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         if (ctx.isOriginLocal()) {
            acquireRemoteIfNeeded(ctx, command.getMap().keySet());
         }
         for (Object key : command.getMap().keySet()) {
            if (cll.localNodeIsOwner(key)) {
               lockKey(ctx, key);
            }
            entryFactory.wrapEntryForPut(ctx, key, true);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         assertNoRemoteContext(ctx);
         acquireRemoteIfNeeded(ctx, Collections.singleton(command.getKey()));
         if (cll.localNodeIsOwner(command.getKey())) {
            lockKey(ctx, command.getKey());
         }
         invokeNextInterceptor(ctx, command);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         assertNoRemoteContext(ctx);
         acquireRemoteIfNeeded(ctx, Collections.singleton(command.getKey()));
         if (cll.localNodeIsOwner(command.getKey())) {
            lockKey(ctx, command.getKey());
         }
         invokeNextInterceptor(ctx, command);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   private void acquireRemoteIfNeeded(InvocationContext ctx, Set<Object> singleton) throws Throwable {
      if (!ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         LockControlCommand lcc = cf.buildLockControlCommand(singleton, true, ctx.getFlags());
         visitLockControlCommand((TxInvocationContext) ctx, lcc);
      }
   }

   private void assertNoRemoteContext(InvocationContext ctx) {
      if (!ctx.isOriginLocal()) throw new IllegalStateException("This shouldn't be called with a remote context!");
   }
}
