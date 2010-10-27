/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Interceptor to implement <a href="http://wiki.jboss.org/wiki/JBossCacheMVCC">MVCC</a> functionality.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @see <a href="http://wiki.jboss.org/wiki/JBossCacheMVCC">MVCC designs</a>
 * @since 4.0
 */
public class LockingInterceptor extends CommandInterceptor {
   LockManager lockManager;
   DataContainer dataContainer;
   EntryFactory entryFactory;
   boolean useReadCommitted;

   @Inject
   public void setDependencies(LockManager lockManager, DataContainer dataContainer, EntryFactory entryFactory) {
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
   }

   @Start
   private void determineIsolationLevel() {
      useReadCommitted = configuration.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (ctx.isInTxScope()) {
            cleanupLocks(ctx, true);
         } else {
            throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional context in scope. " + ctx);
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (ctx.isInTxScope()) {
            cleanupLocks(ctx, false);
         } else {
            throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional context in scope. " + ctx);
         }
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (TimeoutException te) {
         cleanupLocks(ctx, false);
         throw te;
      } finally {
         if (command.isOnePhaseCommit())
            cleanupLocks(ctx, true);
      }
   }

   // read commands

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForReading(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand c) throws Throwable {
      boolean localTxScope = ctx.isOriginLocal() && ctx.isInTxScope();
      boolean shouldInvokeOnCluster = false;
      try {
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
            Object result = invokeNextInterceptor(ctx, c);
            try {
               lockKeysForLockCommand(ctx, c);
               result = true;
            } catch (Throwable e) {
               result = false;
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
      } finally {
         if (ctx.isInTxScope()) {
            doAfterCall(ctx);
         } else {
            throw new IllegalStateException("Attempting to lock but there is no transactional context in scope. " + ctx);
         }
      }
   }

   private void lockKeysForLockCommand(TxInvocationContext ctx, LockControlCommand c) throws InterruptedException {
      for (Object key : c.getKeys()) {
         MVCCEntry e = entryFactory.wrapEntryForWriting(ctx, key, true, false, false, false, false);
         if (e.isCreated()) {
            // mark as temporary entry just for the sake of a lock command
            e.setLockPlaceholder(true);
         }
      }
   }

   // write commands

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         // get a snapshot of all keys in the data container
         for (InternalCacheEntry entry : dataContainer.entrySet())
            entryFactory.wrapEntryForWriting(ctx, entry, false, false, false, false, false);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      } finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // ensure keys are properly locked for evict commands
      ctx.setFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
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
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), true, false, false, false, !command.isPutIfAbsent());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         for (Object key : command.getMap().keySet()) {
            entryFactory.wrapEntryForWriting(ctx, key, true, false, false, false, true);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), false, true, false, true, false);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), false, true, false, false, false);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         return cleanLocksAndRethrow(ctx, te);
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @SuppressWarnings("unchecked")
   private void doAfterCall(InvocationContext ctx) {
      // for non-transactional stuff.
      if (!ctx.isInTxScope()) {
         cleanupLocks(ctx, true);
      } else {
         if (trace) log.trace("Transactional.  Not cleaning up locks till the transaction ends.");
         if (useReadCommitted) {
            Map<Object, CacheEntry> lookedUpEntries = ctx.getLookedUpEntries();
            if (lookedUpEntries != null && !lookedUpEntries.isEmpty()) {
               // This should be a Set but we can use an ArrayList instead for efficiency since we know that the elements
               // will always be unique as they are keys from a Map.  Also, we know the maximum size so this ArrayList 
               // should never resize.
               List<Object> keysToRemove = new ArrayList<Object>(lookedUpEntries.size());
               for (Map.Entry<Object, CacheEntry> e : lookedUpEntries.entrySet()) {
                  if (!lockManager.possiblyLocked(e.getValue()) && !possiblyLockedInContext(ctx, e.getKey())) keysToRemove.add(e.getKey());
               }

               if (!keysToRemove.isEmpty()) {
                  if (trace)
                     log.trace("Removing keys {0} since they have not been modified.  Context currently contains {1} keys", keysToRemove, ctx.getLookedUpEntries().size());
                  for (Object key : keysToRemove) ctx.removeLookedUpEntry(key);
                  if (trace) log.trace("After removal, context contains {0} keys", ctx.getLookedUpEntries().size());
               }
            }
         }
      }
   }

   private boolean possiblyLockedInContext(InvocationContext ctx, Object key) {
      if (ctx instanceof LocalTxInvocationContext) {
         return ((LocalTxInvocationContext) ctx).getAffectedKeys().contains(key);
      } else return false;
   }

   private void cleanupLocks(InvocationContext ctx, boolean commit) {
      if (commit) {
         Object owner = ctx.getLockOwner();
         ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
         if (trace) log.trace("Number of entries in context: {0}", entries.size());
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            Object key = e.getKey();
            boolean needToUnlock = lockManager.possiblyLocked(entry);
            // could be null with read-committed
            if (entry != null && entry.isChanged()) {
               commitEntry(entry);
            } else {
               if (trace) log.trace("Entry for key {0} is null, not calling commitUpdate", key);
            }

            // and then unlock
            if (needToUnlock && !ctx.hasFlag(Flag.SKIP_LOCKING)) {
               if (trace) log.trace("Releasing lock on [" + key + "] for owner " + owner);
               lockManager.unlock(key);
            }
         }
      } else {
         lockManager.releaseLocks(ctx);
      }
   }

   private Object cleanLocksAndRethrow(InvocationContext ctx, Throwable te) throws Throwable {
      cleanupLocks(ctx, false);
      throw te;
   }

   protected void commitEntry(CacheEntry entry) {
      entry.commit(dataContainer);
   }
}
