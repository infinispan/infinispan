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

import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.jdk8backported.ConcurrentHashMapV8;
import org.infinispan.util.concurrent.locks.RefCountingLock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * An abstract lock container that creates and maintains a new lock per entry
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractPerEntryLockContainer<L extends RefCountingLock> extends AbstractLockContainer<L> {

   // We specifically need a CHMV8, to be able to use methods like computeIfAbsent, computeIfPresent and compute
   protected final ConcurrentHashMapV8<Object, L> locks;

   protected AbstractPerEntryLockContainer(int concurrencyLevel) {
      locks = new ConcurrentHashMapV8<Object, L>(16, concurrencyLevel);
   }

   protected abstract L newLock();

   @Override
   public final L getLock(Object key) {
      return locks.get(key);
   }

   @Override
   public int getNumLocksHeld() {
      return locks.size();
   }

   @Override
   public int size() {
      return locks.size();
   }

   private boolean tryIncrementReferences(RefCountingLock lock) {
      int newCount;
      int oldCount;
      do {
         oldCount = lock.getReferenceCounter().get();
         if (oldCount == 0) return false;
         newCount = oldCount + 1;
      } while (!lock.getReferenceCounter().compareAndSet(oldCount, newCount));
      return true;
   }

   @Override
   public L acquireLock(final Object lockOwner, final Object key, final long timeout, final TimeUnit unit) throws InterruptedException {
      L lock = null;
      final AtomicBoolean newLockCreatedAndAcquired = new AtomicBoolean(false);
      while (lock == null) {
         lock = locks.computeIfAbsent(key, new ConcurrentHashMapV8.Fun<Object, L>() {
            @Override
            public L apply(Object key) {
               getLog().tracef("Creating and acquiring new lock instance for key %s", key);
               // Create a new lock here.  This happens atomically if the mapping doesn't exist.
               L lock = newLock();
               // Since this is a new lock, it is certainly uncontended.
               lock(lock, lockOwner);
               newLockCreatedAndAcquired.set(true);
               return lock;
            }
         });
      }

      boolean locked = false;
      boolean refCounterIncremented = false;
      try {
         boolean attemptLock = true;
         while (attemptLock) {
            if (!newLockCreatedAndAcquired.get()) {
               while (!tryIncrementReferences(lock)) {
                  // If we get here, the lock has been removed from the map.
                  // Set the ref count to 1 first
                  if (lock.getReferenceCounter().compareAndSet(0, 1)) {
                     // Now reuse the lock, replace it in the lock map.
                     L existing = locks.putIfAbsent(key, lock);
                     if (existing != null && existing != lock)
                        lock = existing;
                     else
                        break;
                  }
               }
               locked = tryLock(lock, timeout, unit, lockOwner);
            } else {
               locked = true;
            }
            refCounterIncremented = true;
            if (locked) {

               // now that we have the lock, we can decrement the ref counter
               lock.getReferenceCounter().decrementAndGet();
               refCounterIncremented = false;

               // ensure that we replace the lock in the lock map, just in case it is removed.
               // This should be an atomic operation to make sure it isn't interleaved with a concurrent removal of a
               // lock in the map.  If we have acquired a lock, it means no one else can remove it at this point; the
               // only risk is a concurrent remove that started *before* we acquired the lock, that may have released
               // the lock but not finished removing the lock from the map.  Here we rely on the CHM's atomic operation
               // to make sure that the remove operation completes before this check starts.

               final L acquiredLock = lock;
               L existing = locks.compute(key, new ConcurrentHashMapV8.BiFun<Object, L, L>() {
                  @Override
                  public L apply(Object key, L lockInMap) {
                     // Similar to a putIfAbsent, except that the absent check is also atomic wrt. the reinstating of the mapping
                     if (lockInMap == null) {
                        // we have a concurrent remove.  Return the lock we have acquired, to reinstate the mapping!
                        return acquiredLock;
                     } else {
                        return lockInMap;
                     }
                  }
               });
               if (existing != null && existing != lock) {
                  // we have the wrong lock!
                  safeRelease(lock, lockOwner);
                  locked = false;
                  lock = existing;
               } else {
                  attemptLock = false;
               }
            } else {
               attemptLock = false;
               if (refCounterIncremented) lock.getReferenceCounter().decrementAndGet();
            }
         }
      } catch (InterruptedException ie) {
         getLog().tracef("Interrupted when trying to lock %s", key);
         safeRelease(lock, lockOwner);
         if (refCounterIncremented) lock.getReferenceCounter().decrementAndGet();
         throw ie;
      } catch (Throwable th) {
         getLog().tracef("Caught a Throwable when trying to lock %s", key, th);
         if (refCounterIncremented) lock.getReferenceCounter().decrementAndGet();
         locked = false;
      }

      if (locked)
         return lock;
      else {
         getLog().tracef("Timed out attempting to acquire lock for key %s after %s", key, Util.prettyPrintTime(timeout, unit));
         return null;
      }
   }

   @Override
   public void releaseLock(final Object lockOwner, Object key) {
      locks.computeIfPresent(key, new ConcurrentHashMapV8.BiFun<Object, L, L>() {
         @Override
         public L apply(Object key, L value) {
            // This will happen atomically in the CHM
            boolean remove = false;
            if (value != null) {
               remove = value.getReferenceCounter().get() == 0;
               unlock(value, lockOwner);
               // At this point, waiting threads *may* acquire the lock.  See acquireLock() for the check to reinstate
               // the lock in the CHM if the removal happens *after* the lock is acquired by a competing thread.
               getLog().tracef("Unlocking lock instance for key %s", key);
            }
            // Ok, unlock was successful.  If the unlock was not successful, an exception will propagate and the entry will not be changed.
            return remove ? null : value;
         }
      });
   }

   @Override
   public int getLockId(Object key) {
      L lock = getLock(key);
      return lock == null ? -1 : System.identityHashCode(lock);
   }

   @Override
   public String toString() {
      return "AbstractPerEntryLockContainer{" +
            "locks=" + locks +
            '}';
   }
}
