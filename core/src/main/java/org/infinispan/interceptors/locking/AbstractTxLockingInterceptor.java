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

import org.infinispan.CacheException;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Base class for transaction based locking interceptors.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.1
 */
public class AbstractTxLockingInterceptor extends AbstractLockingInterceptor {

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   protected final void abortIfRemoteTransactionInvalid(TxInvocationContext ctx, AbstractTransactionBoundaryCommand c) {
      // this check fixes ISPN-777
      if (!ctx.isOriginLocal()) {
         Address origin = c.getGlobalTransaction().getAddress();
         if (!transport.getMembers().contains(origin))
            throw new CacheException("Member " + origin + " no longer in cluster. Forcing tx rollback for " + c.getGlobalTransaction());
      }
   }

   protected final Object invokeNextAndCommitIf1Pc(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) {
         lockManager.unlockAll(ctx);
      }
      return result;
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
            lockManager.unlockAll(ctx);
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
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   private void lockKeysForLockCommand(TxInvocationContext ctx, LockControlCommand c) throws InterruptedException {
      for (Object key : c.getKeys()) {
         lockKey(ctx, key);
      }
   }
}
