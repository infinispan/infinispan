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
package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.context.InvocationContext;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A container for locks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface LockContainer<L extends Lock> {
   /**
    * Tests if a give owner owns a lock on a specified object.
    *
    * @param key   object to check
    * @param owner owner to test
    * @return true if owner owns lock, false otherwise
    */
   boolean ownsLock(Object key, Object owner);

   /**
    * @param key object
    * @return true if an object is locked, false otherwise
    */
   boolean isLocked(Object key);

   /**
    * @param key object
    * @return the lock for a specific object
    */
   L getLock(Object key);

   /**
    * @return number of locks held
    */
   int getNumLocksHeld();

   /**
    * @return the size of the shared lock pool
    */
   int size();

   /**
    * Attempts to acquire a lock for the given object within certain time boundaries defined by the timeout and
    * time unit parameters.
    *
    * @param key Object to acquire lock on
    * @param timeout Time after which the lock acquisition will fail
    * @param unit Time unit of the given timeout
    * @return If lock was acquired it returns the corresponding Lock object. If lock was not acquired, it returns null
    * @throws InterruptedException If the lock acquisition was interrupted
    */
   L acquireLock(InvocationContext ctx, Object key, long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Release lock on the given key.
    *
    * @param key Object on which lock is to be removed  
    */
   void releaseLock(InvocationContext ctx, Object key);

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
}
