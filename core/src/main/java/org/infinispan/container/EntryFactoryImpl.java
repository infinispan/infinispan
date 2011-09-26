/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.container;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class EntryFactoryImpl implements EntryFactory {
   private boolean useRepeatableRead;
   DataContainer container;
   boolean writeSkewCheck;
   LockManager lockManager;
   Configuration configuration;
   CacheNotifier notifier;

   private static final Log log = LogFactory.getLog(EntryFactoryImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject
   public void injectDependencies(DataContainer dataContainer, LockManager lockManager, Configuration configuration, CacheNotifier notifier) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.lockManager = lockManager;
      this.notifier = notifier;
   }

   @Start
   public void init() {
      useRepeatableRead = configuration.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
      writeSkewCheck = configuration.isWriteSkewCheck();
   }

   private MVCCEntry createWrappedEntry(Object key, Object value, boolean isForInsert, boolean forRemoval, long lifespan) {
      if (value == null && !isForInsert) return useRepeatableRead ?
            forRemoval ? new NullMarkerEntryForRemoval(key) : NullMarkerEntry.getInstance()
            : null;

      return useRepeatableRead ? new RepeatableReadEntry(key, value, lifespan) : new ReadCommittedEntry(key, value, lifespan);
   }

   public final CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      CacheEntry cacheEntry;
      if (ctx.hasFlag(Flag.FORCE_WRITE_LOCK)) {
         if (trace) log.trace("Forcing lock on reading");
         return wrapEntryForWriting(ctx, key, false, false, false, false, false);
      } else if ((cacheEntry = ctx.lookupEntry(key)) == null) {
         if (trace) log.tracef("Key %s is not in context, fetching from container.", key);
         // simple implementation.  Peek the entry, wrap it, put wrapped entry in the context.
         cacheEntry = container.get(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         if (useRepeatableRead && ctx.isInTxScope()) {
            MVCCEntry mvccEntry = cacheEntry == null ?
                  createWrappedEntry(key, null, false, false, -1) :
                  createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
            if (mvccEntry != null) ctx.putLookedUpEntry(key, mvccEntry);
            return mvccEntry;
         }
         // if not in transaction and repeatable read, or simply read committed (regardless of whether in TX or not), do not wrap
         if (cacheEntry != null) ctx.putLookedUpEntry(key, cacheEntry);
         return cacheEntry;
      } else {
         if (trace) log.trace("Key is already in context");
         return cacheEntry;
      }
   }

   public final MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval, boolean undeleteIfNeeded) throws InterruptedException {
      return wrapEntryForWriting(ctx, key, null, createIfAbsent, forceLockIfAbsent, alreadyLocked, forRemoval, undeleteIfNeeded);
   }

   public MVCCEntry wrapEntryForWriting(InvocationContext ctx, InternalCacheEntry entry, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval, boolean undeleteIfNeeded) throws InterruptedException {
      return wrapEntryForWriting(ctx, entry.getKey(), entry, createIfAbsent, forceLockIfAbsent, alreadyLocked, forRemoval, undeleteIfNeeded);
   }

   private MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, InternalCacheEntry entry, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval, boolean undeleteIfNeeded) throws InterruptedException {
      try {
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         MVCCEntry mvccEntry = null;
         if (createIfAbsent && cacheEntry != null && cacheEntry.isNull()) cacheEntry = null;
         if (cacheEntry != null) // exists in context!  Just acquire lock if needed, and wrap.
         {
            if (trace) log.trace("Exists in context.");
            // Acquire lock if needed. Add necessary check for skip locking in advance in order to avoid marshalled value issues
            if (alreadyLocked || ctx.hasFlag(Flag.SKIP_LOCKING) || acquireLock(ctx, key)) {

               if (cacheEntry instanceof MVCCEntry && (!forRemoval || !(cacheEntry instanceof NullMarkerEntry))) {
                  mvccEntry = (MVCCEntry) cacheEntry;
               } else {
                  // this is a read-only entry that needs to be copied to a proper read-write entry!!
                  mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, forRemoval, cacheEntry.getLifespan());
                  cacheEntry = mvccEntry;
                  ctx.putLookedUpEntry(key, cacheEntry);
               }

               // create a copy of the underlying entry
               mvccEntry.copyForUpdate(container, writeSkewCheck);
            } else if (ctx.hasFlag(Flag.FORCE_WRITE_LOCK)) {
               // If lock was already held and force write lock is on, just wrap
               if (cacheEntry instanceof MVCCEntry && (!forRemoval || !(cacheEntry instanceof NullMarkerEntry))) {
                  mvccEntry = (MVCCEntry) cacheEntry;
               }
            }

            if (cacheEntry.isRemoved() && createIfAbsent && undeleteIfNeeded) {
               if (trace) log.trace("Entry is deleted in current scope.  Need to un-delete.");
               if (mvccEntry != cacheEntry) mvccEntry = (MVCCEntry) cacheEntry;
               mvccEntry.setRemoved(false);
               mvccEntry.setValid(true);
            }

            return mvccEntry;

         } else {
            boolean lockAcquired = false;
            if (!alreadyLocked) {
               lockAcquired = acquireLock(ctx, key);
            }
            // else, fetch from dataContainer or used passed entry.
            cacheEntry = entry != null ? entry : container.get(key);
            if (cacheEntry != null) {
               if (trace) log.trace("Retrieved from container.");
               // exists in cache!  Just acquire lock if needed, and wrap.
               // do we need a lock?
               boolean needToCopy = alreadyLocked || lockAcquired || ctx.hasFlag(Flag.SKIP_LOCKING); // even if we do not acquire a lock, if skip-locking is enabled we should copy
               mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
               ctx.putLookedUpEntry(key, mvccEntry);
               if (needToCopy) mvccEntry.copyForUpdate(container, writeSkewCheck);
            } else if (createIfAbsent) {
               // this is the *only* point where new entries can be created!!
               if (trace) log.trace("Creating new entry.");
               // now to lock and create the entry.  Lock first to prevent concurrent creation!
               try {
                  notifier.notifyCacheEntryCreated(key, true, ctx);
               } catch (CacheException e) {
                  // If any exception, release the lock cos the locking
                  // interceptor cannot do it due to the key not being in the
                  // ctx looked up entries yet
                  releaseLock(ctx, key);
                  throw e;
               }
               mvccEntry = createWrappedEntry(key, null, true, false, -1);
               mvccEntry.setCreated(true);
               ctx.putLookedUpEntry(key, mvccEntry);
               mvccEntry.copyForUpdate(container, writeSkewCheck);
               notifier.notifyCacheEntryCreated(key, false, ctx);
            } else {
               if (lockAcquired) {
                  releaseLock(ctx, key);
               }
            }
         }

         // see if we need to force the lock on nonexistent entries.
         if (mvccEntry == null && forceLockIfAbsent) {
            // make sure we record this! Null value since this is a forced lock on the key
            if (acquireLock(ctx, key)) ctx.putLookedUpEntry(key, null);
         }

         return mvccEntry;
      } catch (InvalidTransactionException ite) {
         try {
            releaseLock(ctx, key);
         } catch (Exception e) {
            // may not be necessary?
         }
         throw ite;
      }
   }

   /**
    * Attempts to lock an entry if the lock isn't already held in the current scope, and records the lock in the
    * context.
    *
    * @param ctx context
    * @param key Key to lock
    * @return true if a lock was needed and acquired, false if it didn't need to acquire the lock (i.e., lock was
    *         already held)
    * @throws InterruptedException if interrupted
    * @throws org.infinispan.util.concurrent.TimeoutException
    *                              if we are unable to acquire the lock after a specified timeout.
    */
   public final boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      // don't EVER use lockManager.isLocked() since with lock striping it may be the case that we hold the relevant
      // lock which may be shared with another key that we have a lock for already.
      // nothing wrong, just means that we fail to record the lock.  And that is a problem.
      // Better to check our records and lock again if necessary.

      boolean shouldSkipLocking = ctx.hasFlag(Flag.SKIP_LOCKING);

      if (!ctx.hasLockedKey(key) && !shouldSkipLocking) {
         if (lockManager.lockAndRecord(key, ctx)) {
            return true;
         } else {
            Object owner = lockManager.getOwner(key);
            // if lock cannot be acquired, expose the key itself, not the marshalled value
            if (key instanceof MarshalledValue) {
               key = ((MarshalledValue) key).get();
            }
            throw new TimeoutException("Unable to acquire lock after [" + Util.prettyPrintTime(getLockAcquisitionTimeout(ctx)) + "] on key [" + key + "] for requestor [" +
                  ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
         }
      } else {
         if (trace) {
            if (shouldSkipLocking)
               log.trace("SKIP_LOCKING flag used!");
            else
               log.trace("Already own lock for entry");
         }
      }

      return false;
   }

   //TODO resolve code duplication with org.infinispan.util.concurrent.locks.LockManagerImpl.getLockAcquisitionTimeout(InvocationContext)
   private long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   public final void releaseLock(InvocationContext ctx, Object key) {
      lockManager.unlock(ctx, key);
   }
}
