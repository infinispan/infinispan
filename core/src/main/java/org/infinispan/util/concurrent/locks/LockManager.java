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
package org.infinispan.util.concurrent.locks;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * An interface to deal with all aspects of acquiring and releasing locks for cache entries.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface LockManager {

   /**
    * Acquires a lock of type lockType, on a specific entry in the cache.  This method will try for a period of time and
    * give up if it is unable to acquire the required lock.  The period of time is specified in {@link
    * org.infinispan.config.Configuration#getLockAcquisitionTimeout()}.
    * <p/>
    *
    * @param key key to lock
    * @param ctx invocation context associated with this invocation
    * @return true if the lock was acquired, false otherwise.
    * @throws InterruptedException if interrupted
    */
   boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException;

   /**
    * Releases the lock passed in, held by the specified owner
    *
    * @param owner lock owner
    */
   void unlock(Object key);

   /**
    * Releases locks present in an invocation context and transaction entry, if one is available.
    * <p/>
    * Locks are released in reverse order of which they are acquired and registered.
    * <p/>
    *
    * @param ctx invocation context to inspect
    */
   void unlock(InvocationContext ctx);

   /**
    * Tests whether a given owner owns a lock of lockType on a particular cache entry.
    *
    * @param owner owner
    * @return true if the owner does own the specified lock type on the specified cache entry, false otherwise.
    */
   boolean ownsLock(Object key, Object owner);

   /**
    * Returns true if the cache entry is locked (either for reading or writing) by anyone, and false otherwise.
    *
    * @return true of locked; false if not.
    */
   boolean isLocked(Object key);

   /**
    * Retrieves the write lock owner, if any, for the specified cache entry.
    *
    * @return the owner of the lock, or null if not locked.
    */
   Object getOwner(Object key);

   /**
    * Prints lock information for all locks.
    *
    * @return lock information
    */
   String printLockInfo();

   /**
    * Inspects the entry for signs that it is possibly locked, and hence would need to be unlocked.  Note that this is
    * not deterministic, and is pessimistic in that even if an entry is not locked but *might* be locked, this will
    * return true.
    * <p/>
    * As such, this should only be used to determine whether *unlocking* is necessary, not whether locking is necessary.
    * Unlocking an entry that has not been locked has no effect, so this is just an optimisation.
    * <p/>
    *
    * @param entry entry to inspect
    * @return true if the entry *might* be locked, false if the entry definitely is *not* locked.
    */
   boolean possiblyLocked(CacheEntry entry);

   /**
    * Retrieves the number of locks currently held.
    * @return an integer
    */
   int getNumberOfLocksHeld();

   /**
    * Returns the 'id' of the lock that will be used to guard access to a given key in the cache.  Particularly useful
    * if Lock Striping is used and locks may guard more than one key.  This mechanism can be used to check whether
    * keys may end up sharing the same lock.
    * <p />
    * If lock-striping is not used, the identity hash code of the lock created for this specific key is returned.  While
    * this may not be of much value, it is done to maintain API compatibility of this method regardless of underlying
    * locking scheme.
    *
    * @param key key to test for
    * @return the ID of the lock.
    */
   int getLockId(Object key);

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
   boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException;

   /**
    * Same as {@link #acquireLock(org.infinispan.context.InvocationContext, Object)}, but doesn't check whether the
    * lock is already acquired by the caller. Useful in the case of transactions that use {@link OwnableReentrantLock}s
    * ,as these locks already perform this check internally.
    */
   boolean acquireLockNoCheck(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException;

}
