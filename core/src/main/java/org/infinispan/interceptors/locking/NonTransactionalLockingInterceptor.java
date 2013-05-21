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

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class NonTransactionalLockingInterceptor extends AbstractLockingInterceptor {

   private static final Log log = LogFactory.getLog(NonTransactionalLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);//possibly needed because of L1 locks being acquired
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      assertNonTransactional(ctx);
      boolean skipLocking = hasSkipLocking(command);
      long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
      for (Object key: dataContainer.keySet()) {
         if (shouldLock(key, command)) {
            lockKey(ctx, key, lockTimeout, skipLocking);
         }
      }
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         if (!command.isForwarded()) {
            boolean skipLocking = hasSkipLocking(command);
            long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
            for (Object key : command.getMap().keySet()) {
               if (shouldLock(key, command))
                  lockKey(ctx, key, lockTimeout, skipLocking);
            }
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
      finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         if (!shouldLock(command.getKey(), command))
            return invokeNextInterceptor(ctx, command);
         lockKey(ctx, command);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      assertNonTransactional(ctx);
      try {
         if (!shouldLock(command.getKey(), command))
            return invokeNextInterceptor(ctx, command);
         lockKey(ctx, command);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
      finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public final Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // ensure keys are properly locked for evict commands
      command.setFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
      return visitRemoveCommand(ctx, command);
   }

   private void assertNonTransactional(InvocationContext ctx) {
      //this only happens if the cache is used in a transaction's scope
      if (ctx.isInTxScope()) {
         throw new InvalidCacheUsageException(
               "This is a non-transactional cache and cannot be accessed with a transactional InvocationContext.");
      }
   }
}