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
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.OptimisticEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;

/**
 * Locking interceptor to be used by optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class OptimisticLockingInterceptor extends AbstractTxLockingInterceptor {

   OptimisticEntryFactory entryFactory;

   private final LockAquisitionVisitor lockAquisitionVisitor = new LockAquisitionVisitor();

   @Inject
   public void setDependencies(EntryFactory entryFactory) {
      this.entryFactory = (OptimisticEntryFactory) entryFactory;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         abortIfRemoteTransactionInvalid(ctx, command);
         for (WriteCommand wc : command.getModifications()) {
            wc.acceptVisitor(ctx, lockAquisitionVisitor);
         }
         return invokeNextAndCommitIf1Pc(ctx, command);
      } catch (Throwable te) {
         lockManager.releaseLocks(ctx);
         throw te;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForPut(ctx, command.getKey(), !command.isPutIfAbsent());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         for (Object key : command.getMap().keySet()) {
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
         entryFactory.wrapEntryForRemove(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForReplace(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         for (Object key : dataContainer.keySet())
            entryFactory.wrapEntryForClear(ctx, key);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   private final class LockAquisitionVisitor extends AbstractVisitor {

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         boolean notWrapped = false;
         for (Object key : dataContainer.keySet()) {
            lockKey(ctx, key);
            if (notWrapped(ctx, key)) {
               entryFactory.wrapEntryForClear(ctx, key);
               notWrapped = true;
            }
         }
         if (notWrapped)
            invokeNextInterceptor(ctx, command);
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         boolean notWrapped = false;
         for (Object key : command.getMap().keySet()) {
            if (cll.localNodeIsOwner(key)) {
               lockKey(ctx, key);
               if (notWrapped(ctx, key)) {
                  entryFactory.wrapEntryForPut(ctx, key, true);
                  notWrapped = true;
               }
            }
         }
         if (notWrapped)
            invokeNextInterceptor(ctx, command);
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            lockKey(ctx, command.getKey());
            if (notWrapped(ctx, command.getKey())) {
               entryFactory.wrapEntryForRemove(ctx, command.getKey());
               invokeNextInterceptor(ctx, command);
            }
         }
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            lockKey(ctx, command.getKey());
            if (notWrapped(ctx, command.getKey())) {
               entryFactory.wrapEntryForPut(ctx, command.getKey(), !command.isPutIfAbsent());
               invokeNextInterceptor(ctx, command);
            }
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            lockKey(ctx, command.getKey());
            if (notWrapped(ctx, command.getKey())) {
               entryFactory.wrapEntryForReplace(ctx, command.getKey());
               invokeNextInterceptor(ctx, command);
            }
         }
         return null;
      }

      private boolean notWrapped(InvocationContext ctx, Object key) {
         return !ctx.getLookedUpEntries().containsKey(key);
      }
   }
}
