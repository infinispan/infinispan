/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.horizon.factories;

import org.horizon.config.Configuration;
import org.horizon.container.DataContainer;
import org.horizon.container.entries.CacheEntry;
import org.horizon.container.entries.MVCCEntry;
import org.horizon.container.entries.NullMarkerEntry;
import org.horizon.container.entries.NullMarkerEntryForRemoval;
import org.horizon.container.entries.ReadCommittedEntry;
import org.horizon.container.entries.RepeatableReadEntry;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.invocation.Flag;
import org.horizon.lock.IsolationLevel;
import org.horizon.lock.LockManager;
import org.horizon.lock.TimeoutException;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.notifications.cachelistener.CacheNotifier;

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
         return wrapEntryForWriting(ctx, key, false, false, false, false);
      } else if ((cacheEntry = ctx.lookupEntry(key)) == null) {
         if (trace) log.trace("Key {0} is not in context, fetching from container.", key);
         // simple implementation.  Peek the entry, wrap it, put wrapped entry in the context.
         cacheEntry = container.get(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         // TODO: Do we need to wrap for reading even IN a TX if we are using read-committed?
         if (ctx.getTransaction() == null) {
            if (cacheEntry != null) ctx.putLookedUpEntry(key, cacheEntry);
            return cacheEntry;
         } else {
            MVCCEntry mvccEntry = cacheEntry == null ?
                  createWrappedEntry(key, null, false, false, -1) :
                  createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
            if (mvccEntry != null) ctx.putLookedUpEntry(key, mvccEntry);
            return mvccEntry;
         }
      } else {
         if (trace) log.trace("Key is already in context");
         return cacheEntry;
      }
   }

   public final MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval) throws InterruptedException {
      CacheEntry cacheEntry = ctx.lookupEntry(key);
      MVCCEntry mvccEntry = null;
      if (createIfAbsent && cacheEntry != null && cacheEntry.isNull()) cacheEntry = null;
      if (cacheEntry != null) // exists in context!  Just acquire lock if needed, and wrap.
      {
         if (trace) log.trace("Exists in context.");
         // acquire lock if needed
         if (alreadyLocked || acquireLock(ctx, key)) {

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
         }

         if (cacheEntry.isRemoved() && createIfAbsent) {
            if (trace) log.trace("Entry is deleted in current scope.  Need to un-delete.");
            if (mvccEntry != cacheEntry) mvccEntry = (MVCCEntry) cacheEntry;
            mvccEntry.setRemoved(false);
            mvccEntry.setValid(true);
         }         

         return mvccEntry;

      } else {
         // else, fetch from dataContainer.
         cacheEntry = container.get(key);
         if (cacheEntry != null) {
            if (trace) log.trace("Retrieved from container.");
            // exists in cache!  Just acquire lock if needed, and wrap.
            // do we need a lock?
            boolean needToCopy = alreadyLocked || acquireLock(ctx, key) || ctx.hasFlag(Flag.SKIP_LOCKING); // even if we do not acquire a lock, if skip-locking is enabled we should copy
            mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
            ctx.putLookedUpEntry(key, mvccEntry);
            if (needToCopy) mvccEntry.copyForUpdate(container, writeSkewCheck);
            cacheEntry = mvccEntry;
         } else if (createIfAbsent) {
            // this is the *only* point where new entries can be created!!
            if (trace) log.trace("Creating new entry.");
            // now to lock and create the entry.  Lock first to prevent concurrent creation!
            if (!alreadyLocked) acquireLock(ctx, key);
            notifier.notifyCacheEntryCreated(key, true, ctx);
            mvccEntry = createWrappedEntry(key, null, true, false, -1);
            mvccEntry.setCreated(true);
            ctx.putLookedUpEntry(key, mvccEntry);
            mvccEntry.copyForUpdate(container, writeSkewCheck);
            notifier.notifyCacheEntryCreated(key, false, ctx);
            cacheEntry = mvccEntry;
         }
      }

      // see if we need to force the lock on nonexistent entries.
      if (mvccEntry == null && forceLockIfAbsent) {
         // make sure we record this! Null value since this is a forced lock on the key
         if (acquireLock(ctx, key)) ctx.putLookedUpEntry(key, null);
      }

      return mvccEntry;
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
    * @throws org.horizon.lock.TimeoutException
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
            // successfully locked!
            return true;
         } else {
            Object owner = lockManager.getOwner(key);
            throw new TimeoutException("Unable to acquire lock on key [" + key + "] after [" + getLockAcquisitionTimeout(ctx)
                  + "] milliseconds for requestor [" + lockManager.getLockOwner(ctx) + "]! Lock held by [" + owner + "]");
         }
      }

      return false;
   }

   private long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   public final void releaseLock(Object key) {
      lockManager.unlock(key, lockManager.getOwner(key));
   }
}
