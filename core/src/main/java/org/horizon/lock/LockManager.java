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
package org.horizon.lock;

import org.horizon.context.InvocationContext;
import org.horizon.container.entries.CacheEntry;

/**
 * An interface to deal with all aspects of acquiring and releasing locks for cache entries.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface LockManager {
   /**
    * Determines the owner to be used when obtaining locks, given an invocation context.  This is typically a {@link
    * org.horizon.transaction.GlobalTransaction} if one is present in the context, or {@link Thread#currentThread()} if
    * one is not present.
    *
    * @param ctx invocation context
    * @return owner to be used for acquiring locks.
    */
   Object getLockOwner(InvocationContext ctx);

   /**
    * Acquires a lock of type lockType, on a specific entry in the cache.  This method will try for a period of time and
    * give up if it is unable to acquire the required lock.  The period of time is specified in {@link
    * org.horizon.config.Configuration#getLockAcquisitionTimeout()}.
    * <p/>
    * The owner for the lock is determined by passing the invocation context to {@link
    * #getLockOwner(InvocationContext)}.
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
   void unlock(Object key, Object owner);

   /**
    * Releases locks present in an invocation context and transaction entry, if one is available.
    * <p/>
    * Locks are released in reverse order of which they are acquired and registered.
    * <p/>
    * Lock owner is determined by passing the invocation context to {@link #getLockOwner(InvocationContext)}
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
}
