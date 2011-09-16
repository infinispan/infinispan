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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.infinispan.util.Util.safeRelease;

/**
 * An abstract lock container that creates and maintains a new lock per entry
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractPerEntryLockContainer implements LockContainer {

   protected final ConcurrentMap<Object, Lock> locks;

   protected AbstractPerEntryLockContainer(int concurrencyLevel) {
      locks = new ConcurrentHashMap<Object, Lock>(16, .75f, concurrencyLevel);
   }

   protected abstract Lock newLock();

   public final Lock getLock(Object key) {
      // this is an optimisation.  It is not foolproof as we may still be creating new locks unnecessarily (thrown away
      // when we do a putIfAbsent) but it minimises the chances somewhat, for the cost of an extra CHM get.
      Lock lock = locks.get(key);
      if (lock == null) lock = newLock();
      Lock existingLock = locks.putIfAbsent(key, lock);
      if (existingLock != null) lock = existingLock;
      return lock;
   }

   public int getNumLocksHeld() {
      return locks.size();
   }

   public int size() {
      return locks.size();
   }

   public Lock acquireLock(Object key, long timeout, TimeUnit unit) throws InterruptedException {
      while (true) {
         Lock lock = getLock(key);
         boolean locked = false;
         try {
            locked = lock.tryLock(timeout, unit);
         } catch (InterruptedException ie) {
            safeRelease(lock);
            throw ie;
         } catch (Throwable th) {
             locked = false;
         }
         if (locked) {
            // lock acquired.  Now check if it is the *correct* lock!
            Lock existingLock = locks.putIfAbsent(key, lock);
            if (existingLock != null && existingLock != lock) {
               // we have the wrong lock!  Unlock and retry.
               safeRelease(lock);
            } else {
               // we got the right lock.
               return lock;
            }
         } else {
            // we couldn't acquire the lock within the timeout period
            return null;
         }
      }
   }

   public void releaseLock(Object key) {
      Lock l = locks.remove(key);
      if (l != null) l.unlock();
   }

   public int getLockId(Object key) {
      return System.identityHashCode(getLock(key));
   }
}
