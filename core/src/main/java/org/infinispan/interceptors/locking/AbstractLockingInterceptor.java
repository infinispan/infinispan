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

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Base class for various locking interceptors in this package.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractLockingInterceptor extends CommandInterceptor {

   protected LockManager lockManager;
   protected DataContainer dataContainer;
   protected EntryFactory entryFactory;
   protected ClusteringDependentLogic cdl;

   @Inject
   public void setDependencies(LockManager lockManager, DataContainer dataContainer, EntryFactory entryFactory, ClusteringDependentLogic cdl) {
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
      this.cdl = cdl;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
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
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      try {
         boolean skipLocking = hasSkipLocking(command);
         long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
         for (Object key : command.getKeys()) {
            lockKey(ctx, key, lockTimeout, skipLocking);
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   @Override
   public final Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      if (command.isCausedByALocalWrite(cdl.getAddress())) {
         getLog().trace("Skipping invalidation as the write operation originated here.");
         return null;
      }

      Object[] keys = command.getKeys();
      try {
         if (keys != null && keys.length >= 1) {
            ArrayList<Object> keysCopy = new ArrayList<Object>(Arrays.asList(keys));
            boolean skipLocking = hasSkipLocking(command);
            for (Object key : command.getKeys()) {
               try {
                  lockKey(ctx, key, 0, skipLocking);
               } catch (TimeoutException te) {
                  getLog().unableToLockToInvalidate(key, cdl.getAddress());
                  keysCopy.remove(key);
                  if (keysCopy.isEmpty())
                     return null;
               }
            }
            command.setKeys(keysCopy.toArray());
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
      finally {
         command.setKeys(keys);
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   protected final Throwable cleanLocksAndRethrow(InvocationContext ctx, Throwable te) {
      lockManager.unlockAll(ctx);
      return te;
   }

   protected final boolean shouldLock(Object key, FlagAffectedCommand command) {
      if (hasSkipLocking(command))
         return false;
      if (cacheConfiguration.clustering().cacheMode() == CacheMode.LOCAL)
         return true;
      boolean shouldLock = cdl.localNodeIsPrimaryOwner(key);
      getLog().tracef("Are (%s) we the lock owners for key '%s'? %s", cdl.getAddress(), key, shouldLock);
      return shouldLock;
   }

   protected final void lockKey(InvocationContext ctx, DataWriteCommand command) throws InterruptedException {
      boolean skipLocking = hasSkipLocking(command);
      long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
      lockKey(ctx, command.getKey(), lockTimeout, skipLocking);
   }

   protected final void lockKey(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException {
      lockManager.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);
   }

}
