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

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Transport;
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
public class AbstractLockingInterceptor extends CommandInterceptor {

   LockManager lockManager;
   DataContainer dataContainer;
   EntryFactory entryFactory;
   Transport transport;
   ClusteringDependentLogic cll;

   @Inject
   public void setDependencies(LockManager lockManager, DataContainer dataContainer, EntryFactory entryFactory, Transport transport, ClusteringDependentLogic cll) {
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
      this.transport = transport;
      this.cll = cll;
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      try {
         for (Object key : command.getKeys()) {
            lockKey(ctx, key);
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         releaseLocksIfNoTransaction(ctx);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         for (InternalCacheEntry entry : dataContainer.entrySet())
            lockKey(ctx, entry.getKey());

         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      } finally {
         // for non-transactional stuff.
         releaseLocksIfNoTransaction(ctx);
      }
   }

   @Override
   public final Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      Object keys [] = command.getKeys();
      try {
         if (keys != null && keys.length >= 1) {
            ArrayList<Object> keysCopy = new ArrayList<Object>(Arrays.asList(keys));
            for (Object key : command.getKeys()) {
               ctx.setFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
               try {
                  lockKey(ctx, key);
               } catch (TimeoutException te) {
                  log.unableToLockToInvalidate(key, transport.getAddress());
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
         releaseLocksIfNoTransaction(ctx);
      }
   }

   private void releaseLocksIfNoTransaction(InvocationContext ctx) {
      if (!ctx.isInTxScope()) {
         lockManager.unlock(ctx);
      } else {
         if (trace) log.trace("Transactional.  Not cleaning up locks till the transaction ends.");
      }
   }

   protected final Throwable cleanLocksAndRethrow(InvocationContext ctx, Throwable te) {
      lockManager.unlock(ctx);
      return te;
   }

   protected final void lockKey(InvocationContext ctx, Object key) throws InterruptedException {
      lockManager.acquireLockNoCheck(ctx, key);
   }
}
