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

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.SizeCommand;
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
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.EntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Interceptor to implement <a href="http://wiki.jboss.org/wiki/JBossCacheMVCC">MVCC</a> functionality.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
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
   public Object visitCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         transactionalCleanup(true, ctx);
      }
   }

   @Override
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         transactionalCleanup(false, ctx);
      }
   }

   @Override
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (command.isOnePhaseCommit()) transactionalCleanup(true, ctx);
      }
   }

   // read commands

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForReading(ctx, command.getKey());
         Object r = invokeNextInterceptor(ctx, command);
         return r;
      } finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      try {
         if (log.isDebugEnabled()) log.debug("No locking performed for SizeCommands");
         return invokeNextInterceptor(ctx, command);
      } finally {
         doAfterCall(ctx);
      }
   }

   // write commands

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         // get a snapshot of all keys in the data container
         for (Object key : dataContainer.keySet())
            entryFactory.wrapEntryForWriting(ctx, key, false, false, false, false);
         ctx.setContainsModifications(true);
         return invokeNextInterceptor(ctx, command);
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
            for (Object key : command.getKeys()) entryFactory.wrapEntryForWriting(ctx, key, false, true, false, false);
         }
         Object o = invokeNextInterceptor(ctx, command);
         if (!ctx.isContainsModifications()) ctx.setContainsModifications(command.isSuccessful());
         return o;
      } finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), true, false, false, false);
         Object o = invokeNextInterceptor(ctx, command);
         if (!ctx.isContainsModifications()) ctx.setContainsModifications(command.isSuccessful());
         return o;
      } finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         for (Object key : command.getMap().keySet()) {
            entryFactory.wrapEntryForWriting(ctx, key, true, false, false, false);
         }
         Object o = invokeNextInterceptor(ctx, command);
         if (!ctx.isContainsModifications()) ctx.setContainsModifications(command.isSuccessful());
         return o;
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), false, true, false, true);
         Object o = invokeNextInterceptor(ctx, command);
         if (!ctx.isContainsModifications()) ctx.setContainsModifications(command.isSuccessful());
         return o;
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), false, true, false, false);
         Object o = invokeNextInterceptor(ctx, command);
         if (!ctx.isContainsModifications()) ctx.setContainsModifications(command.isSuccessful());
         return o;
      }
      finally {
         doAfterCall(ctx);
      }
   }

   @SuppressWarnings("unchecked")
   private void doAfterCall(InvocationContext ctx) {
      // for non-transactional stuff.
      if (ctx.getTransactionContext() == null) {
         if (ctx.isContainsModifications() || ctx.isContainsLocks()) {
            cleanupLocks(ctx, Thread.currentThread(), true);
         } else {
            if (trace) log.trace("Nothing to do since there are no modifications in scope.");
         }
      } else {
         if (trace) log.trace("Transactional.  Not cleaning up locks till the transaction ends.");
         if (useReadCommitted) {
            Map<Object, CacheEntry> lookedUpEntries = ctx.getLookedUpEntries();
            if (!lookedUpEntries.isEmpty()) {
               // This should be a Set but we can use an ArrayList instead for efficiency since we know that the elements
               // will always be unique as they are keys from a Map.  Also, we know the maximum size so this ArrayList 
               // should never resize.
               List<Object> keysToRemove = new ArrayList<Object>(lookedUpEntries.size());
               for (Map.Entry<Object, CacheEntry> e : lookedUpEntries.entrySet()) {
                  if (!lockManager.possiblyLocked(e.getValue())) keysToRemove.add(e.getKey());
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

   private void cleanupLocks(InvocationContext ctx, Object owner, boolean commit) {
      // clean up.
      // unlocking needs to be done in reverse order.
      ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
      Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
      if (trace) log.trace("Number of entries in context: {0}", entries.size());

      if (commit) {
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            Object key = e.getKey();
            boolean needToUnlock = lockManager.possiblyLocked(entry);
            // could be null with read-committed
            if (entry != null && entry.isChanged()) {
               commitEntry(ctx, entry);
            } else {
               if (trace) log.trace("Entry for key {0} is null, not calling commitUpdate", key);
            }

            // and then unlock
            if (needToUnlock) {
               if (trace) log.trace("Releasing lock on [" + key + "] for owner " + owner);
               lockManager.unlock(key, owner);
            }
         }
      } else {
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            Object key = e.getKey();
            boolean needToUnlock = lockManager.possiblyLocked(entry);
            // could be null with read-committed
            if (entry != null && entry.isChanged()) entry.rollback();
            else {
               if (trace) log.trace("Entry for key {0} is null, not calling rollbackUpdate", key);
            }
            // and then unlock
            if (needToUnlock) {
               if (trace) log.trace("Releasing lock on [" + key + "] for owner " + owner);
               lockManager.unlock(key, owner);
            }
         }
      }
      ctx.setContainsModifications(false);
      ctx.setContainsLocks(false);
   }

   protected void commitEntry(InvocationContext ctx, CacheEntry entry) {
      entry.commit(dataContainer);
   }

   @SuppressWarnings("unchecked")
   private void transactionalCleanup(boolean commit, InvocationContext ctx) {
      if (ctx.getTransactionContext() != null) {
         if (ctx.isContainsModifications() || ctx.isContainsLocks()) {
            if (trace)
               log.trace("Performing cleanup.  Contains mods? {0} Contains locks? {1}", ctx.isContainsModifications(), ctx.isContainsLocks());
            cleanupLocks(ctx, ctx.getGlobalTransaction(), commit);
         } else {
            if (trace)
               log.trace("At transaction boundary (" + (commit ? "commit" : "rollback") + "), and we have no locks in context!");
         }
      } else {
         throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional context in scope. " + ctx);
      }
   }
}
