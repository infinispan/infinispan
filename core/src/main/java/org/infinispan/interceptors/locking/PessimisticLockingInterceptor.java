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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
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
         //just apply the changes, no need to acquire locks as this has already happened
         for (WriteCommand c : command.getModifications()) {
            invokeNextInterceptor(ctx, c);
         }
         return invokeNextAndCommitIf1Pc(ctx, command);
      } catch (Throwable t) {
         lockManager.releaseLocks(ctx);
         throw t;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (ctx.isOriginLocal()) {
            acquireRemoteIfNeeded(ctx, Collections.singleton(command.getKey()));
            if (cll.localNodeIsOwner(command.getKey())) {
               lockForPut(ctx, command.getKey(), !command.isPutIfAbsent());
            }
         } else {
            lockForPut(ctx, command.getKey(), !command.isPutIfAbsent());
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
            for (Object key : command.getMap().keySet()) {
               if (cll.localNodeIsOwner(key))
                  lockForPut(ctx, key, true);
            }
         } else {
            for (Object key : command.getMap().keySet()) {
               lockForPut(ctx, key, true);
            }
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         if (ctx.isOriginLocal()) {
            acquireRemoteIfNeeded(ctx, Collections.singleton(command.getKey()));
            if (cll.localNodeIsOwner(command.getKey()))
               lockForRemove(ctx, command);
         } else {
            lockForRemove(ctx, command);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         if (ctx.isOriginLocal()) {
            acquireRemoteIfNeeded(ctx, Collections.singleton(command.getKey()));
            if (cll.localNodeIsOwner(command.getKey()))
               lockForReplace(ctx, command);
         } else {
            lockForReplace(ctx, command);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
   }

   private Object lockEagerly(InvocationContext ctx, Collection<Object> keys) throws Throwable {
      LockControlCommand lcc = cf.buildLockControlCommand(keys, true, ctx.getFlags());
      return visitLockControlCommand((TxInvocationContext) ctx, lcc);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand c) throws Throwable {
      boolean localTxScope = ctx.isOriginLocal() && ctx.isInTxScope();
      boolean shouldInvokeOnCluster = false;

      try {
         abortIfRemoteTransactionInvalid(ctx, c);
         if (localTxScope) {
            c.attachGlobalTransaction((GlobalTransaction) ctx.getLockOwner());
         }

         if (c.isUnlock()) {
            lockManager.releaseLocks(ctx);
            if (log.isTraceEnabled()) log.trace("Lock released for: " + ctx.getLockOwner());
            return false;
         }

         for (Object key : c.getKeys()) {
            if (c.isImplicit() && localTxScope && !lockManager.ownsLock(key, ctx.getLockOwner())) {
               //if even one key is unlocked we need to invoke this lock command cluster wide...
               shouldInvokeOnCluster = true;
               break;
            }
         }
         boolean goRemoteFirst = configuration.isEnableDeadlockDetection() && localTxScope;
         if (goRemoteFirst) {
            Object result;
            invokeNextInterceptor(ctx, c);
            try {
               lockKeysForLockCommand(ctx, c);
               result = true;
            } catch (Throwable e) {
               //if anything happen during locking then unlock remote
               c.setUnlock(true);
               invokeNextInterceptor(ctx, c);
               throw e;
            }
            return result;
         } else {
            lockKeysForLockCommand(ctx, c);
            if (shouldInvokeOnCluster || c.isExplicit()) {
               invokeNextInterceptor(ctx, c);
               return true;
            } else {
               return true;
            }
         }
      } catch (Throwable te) {
         cleanLocksAndRethrow(ctx, te);
         return false;
      }
   }

   private void lockKeysForLockCommand(TxInvocationContext ctx, LockControlCommand c) throws InterruptedException {
      for (Object key : c.getKeys()) {
         MVCCEntry e = entryFactory.wrapEntryForWriting(ctx, key, true, false, false, false, false);
         if (e != null && e.isCreated()) {
            // mark as temporary entry just for the sake of a lock command
            e.setLockPlaceholder(true);
         }
      }
   }


   private void acquireRemoteIfNeeded(InvocationContext ctx, Set<Object> singleton) throws Throwable {
      if (!ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         lockEagerly(ctx, singleton);
      }
   }
}
