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
import org.horizon.container.CachedValue;
import org.horizon.container.DataContainer;
import org.horizon.container.MVCCEntry;
import org.horizon.container.NullMarkerEntry;
import org.horizon.container.NullMarkerEntryForRemoval;
import org.horizon.container.ReadCommittedEntry;
import org.horizon.container.RepeatableReadEntry;
import org.horizon.container.UpdateableEntry;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.invocation.Options;
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

   private UpdateableEntry createWrappedEntry(Object key, Object value, boolean isForInsert, boolean forRemoval, long lifespan) {
      if (value == null && !isForInsert) return useRepeatableRead ?
            forRemoval ? new NullMarkerEntryForRemoval(key) : NullMarkerEntry.getInstance()
            : null;

      return useRepeatableRead ? new RepeatableReadEntry(key, value, lifespan) : new ReadCommittedEntry(key, value, lifespan);
   }

   public final MVCCEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      MVCCEntry mvccEntry;
      if (ctx.hasOption(Options.FORCE_WRITE_LOCK)) {
         if (trace) log.trace("Forcing lock on reading");
         return wrapEntryForWriting(ctx, key, false, false, false, false);
      } else if ((mvccEntry = ctx.lookupEntry(key)) == null) {
         if (trace) log.trace("Key " + key + " is not in context, fetching from container.");
         // simple implementation.  Peek the entry, wrap it, put wrapped entry in the context.
         CachedValue se = container.getEntry(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         // TODO: Do we need to wrap for reading even IN a TX if we are using read-committed?
         if (ctx.getTransaction() == null) {
            if (se != null) ctx.putLookedUpEntry(key, se);
         } else {
            mvccEntry = se == null ?
                  createWrappedEntry(key, null, false, false, -1) :
                  createWrappedEntry(key, se.getValue(), false, false, se.getLifespan());
            if (mvccEntry != null) ctx.putLookedUpEntry(key, mvccEntry);
         }

         return mvccEntry;
      } else {
         if (trace) log.trace("Key is already in context");
         return mvccEntry;
      }
   }

   public final MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean createIfAbsent, boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval) throws InterruptedException {
      MVCCEntry mvccEntry = ctx.lookupEntry(key);
      if (createIfAbsent && mvccEntry != null && mvccEntry.isNullEntry()) mvccEntry = null;
      if (mvccEntry != null) // exists in context!  Just acquire lock if needed, and wrap.
      {
         // acquire lock if needed
         if (alreadyLocked || acquireLock(ctx, key)) {
            UpdateableEntry ue;

            if (mvccEntry instanceof UpdateableEntry && (!forRemoval || !(mvccEntry instanceof NullMarkerEntry))) {
               ue = (UpdateableEntry) mvccEntry;
            } else {
               // this is a read-only entry that needs to be copied to a proper read-write entry!!
               ue = createWrappedEntry(key, mvccEntry.getValue(), false, forRemoval, mvccEntry.getLifespan());
               mvccEntry = ue;
               ctx.putLookedUpEntry(key, mvccEntry);
            }

            // create a copy of the underlying entry
            ue.copyForUpdate(container, writeSkewCheck);
         }
         if (trace) log.trace("Exists in context.");
         if (mvccEntry.isDeleted() && createIfAbsent) {
            if (trace) log.trace("Entry is deleted in current scope.  Need to un-delete.");
            mvccEntry.setDeleted(false);
            mvccEntry.setValid(true);
         }
      } else {
         // else, fetch from dataContainer.
         CachedValue cachedValue = container.getEntry(key);
         if (cachedValue != null) {
            if (trace) log.trace("Retrieved from container.");
            // exists in cache!  Just acquire lock if needed, and wrap.
            // do we need a lock?
            boolean needToCopy = alreadyLocked || acquireLock(ctx, key) || ctx.hasOption(Options.SKIP_LOCKING); // even if we do not acquire a lock, if skip-locking is enabled we should copy
            UpdateableEntry ue = createWrappedEntry(key, cachedValue.getValue(), false, false, cachedValue.getLifespan());
            ctx.putLookedUpEntry(key, ue);
            if (needToCopy) ue.copyForUpdate(container, writeSkewCheck);
            mvccEntry = ue;
         } else if (createIfAbsent) {
            // this is the *only* point where new entries can be created!!
            if (trace) log.trace("Creating new entry.");
            // now to lock and create the entry.  Lock first to prevent concurrent creation!
            if (!alreadyLocked) acquireLock(ctx, key);
            notifier.notifyCacheEntryCreated(key, true, ctx);
            UpdateableEntry ue = createWrappedEntry(key, null, true, false, -1);
            ue.setCreated(true);
            ctx.putLookedUpEntry(key, ue);
            ue.copyForUpdate(container, writeSkewCheck);
            notifier.notifyCacheEntryCreated(key, false, ctx);
            mvccEntry = ue;
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

      boolean shouldSkipLocking = ctx.hasOption(Options.SKIP_LOCKING);

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
      return ctx.hasOption(Options.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   public final void releaseLock(Object key) {
      lockManager.unlock(key, lockManager.getOwner(key));
   }
}
