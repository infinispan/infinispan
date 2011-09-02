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

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.OptimisticEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for various locking interceptors in this package.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class AbstractLockingInterceptor extends CommandInterceptor {

   LockManager lockManager;
   DataContainer dataContainer;
   OptimisticEntryFactory entryFactory;
   Transport transport;
   ClusteringDependentLogic cll;

   @Inject
   public void setDependencies(LockManager lockManager, DataContainer dataContainer, EntryFactory entryFactory, Transport transport, ClusteringDependentLogic cll) {
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.entryFactory = (OptimisticEntryFactory) entryFactory;
      this.transport = transport;
      this.cll = cll;
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForReading(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } finally {
         releaseLocksIfNeeded(ctx);
      }
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      try {
         if (command.getKeys() != null) {
            for (Object key : command.getKeys())
               entryFactory.wrapEntryForWriting(ctx, key, false, true, false, false, false);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         releaseLocksIfNeeded(ctx);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         // get a snapshot of all keys in the data container
         lockForClear(ctx);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      } finally {
         // for non-transactional stuff.
         releaseLocksIfNeeded(ctx);
      }
   }

   @Override
   public final Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      Object keys [] = command.getKeys();
      try {
         if (keys != null && keys.length>=1) {
            ArrayList<Object> keysCopy = new ArrayList<Object>(Arrays.asList(keys));
            for (Object key : command.getKeys()) {
               ctx.setFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
               try {
                  entryFactory.wrapEntryForWriting(ctx, key, false, true, false, false, false);
               } catch (TimeoutException te){
            	   log.unableToLockToInvalidate(key,transport.getAddress());
                  keysCopy.remove(key);
                  if(keysCopy.isEmpty())
                     return null;
               }
            }
            command.setKeys(keysCopy.toArray());
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         command.setKeys(keys);
         releaseLocksIfNeeded(ctx);
      }
   }

   private void releaseLocksIfNeeded(InvocationContext ctx) {
      if (!ctx.isInTxScope()) {
         commit(ctx);
      } else {
         if (trace) log.trace("Transactional.  Not cleaning up locks till the transaction ends.");
      }
   }

   protected final void commit(InvocationContext ctx) {
      Object owner = ctx.getLockOwner();
      ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
      Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
      if (trace) log.tracef("Number of entries in context: %s", entries.size());
      while (it.hasNext()) {
         Map.Entry<Object, CacheEntry> e = it.next();
         CacheEntry entry = e.getValue();
         Object key = e.getKey();
         boolean needToUnlock = lockManager.possiblyLocked(entry);
         // could be null with read-committed
         if (entry != null && entry.isChanged()) {
            cll.commitEntry(entry, ctx.hasFlag(Flag.SKIP_OWNERSHIP_CHECK));
         } else {
            if (trace) log.tracef("Entry for key %s is null, not calling commitUpdate", key);
         }

         // and then unlock
         if (needToUnlock && !ctx.hasFlag(Flag.SKIP_LOCKING)) {
            if (trace) log.tracef("Releasing lock on [%s] for owner %s", key, owner);
            lockManager.unlock(key);
         }
      }
   }

   protected final Object cleanLocksAndRethrow(InvocationContext ctx, Throwable te) throws Throwable {
      lockManager.releaseLocks(ctx);
      throw te;
   }

   protected final void lockForClear(InvocationContext ctx) throws InterruptedException {
      for (InternalCacheEntry entry : dataContainer.entrySet())
         entryFactory.wrapEntryForClear(ctx, entry.getKey());
   }

   protected final void lockKey(InvocationContext ctx, Object key) throws InterruptedException {
      lockManager.acquireLockNoCheck(ctx, key);
   }
}
