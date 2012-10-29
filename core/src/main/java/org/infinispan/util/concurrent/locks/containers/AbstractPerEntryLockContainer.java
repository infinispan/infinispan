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

import org.infinispan.util.ByRef;
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
      final ByRef<Boolean> lockAcquired = ByRef.create(Boolean.FALSE);
      L lock = locks.compute(key, new ConcurrentHashMapV8.BiFun<Object, L, L>() {
         @Override
         public L apply(Object key, L lock) {
            // This happens atomically in the CHM
            if (lock == null) {
               getLog().tracef("Creating and acquiring new lock instance for key %s", key);
               lock = newLock();
               // Since this is a new lock, it is certainly uncontended.
               lock(lock, lockOwner);
               lockAcquired.set(Boolean.TRUE);
               return lock;
            }

            // No need to worry about concurrent updates - there can't be a release in progress at the same time
            int refCount = lock.getReferenceCounter().incrementAndGet();
            if (refCount <= 1) {
               throw new IllegalStateException("Lock " + key + " acquired although it should have been removed: " + lock);
            }
            return lock;
         }
      });

      if (!lockAcquired.get()) {
         // We retrieved a lock that was already present,
         lockAcquired.set(tryLock(lock, timeout, unit, lockOwner));
      }

      if (lockAcquired.get())
         return lock;
      else {
         getLog().tracef("Timed out attempting to acquire lock for key %s after %s", key, Util.prettyPrintTime(timeout, unit));

         // We didn't acquire the lock, but we still incremented the reference count.
         // We may need to delete the entry if the owner thread released it just after we timed out.
         // We use an atomic operation here as another thread might be trying to increment the ref count
         // at the same time (otherwise it would make the acquire function at the beginning more complicated).
         locks.computeIfPresent(key, new ConcurrentHashMapV8.BiFun<Object, L, L>() {
            @Override
            public L apply(Object key, L lock) {
               // This will happen atomically in the CHM
               // We have a reference, so value can't be null
               boolean remove = lock.getReferenceCounter().decrementAndGet() == 0;
               return remove ? null : lock;
            }
         });
         return null;
      }
   }

   @Override
   public void releaseLock(final Object lockOwner, Object key) {
      locks.computeIfPresent(key, new ConcurrentHashMapV8.BiFun<Object, L, L>() {
         @Override
         public L apply(Object key, L lock) {
            // This will happen atomically in the CHM
            // We have a reference, so value can't be null
            int refCount = lock.getReferenceCounter().decrementAndGet();
            if (refCount < 0) {
               throw new IllegalStateException("Negative reference count for lock " + key + ": " + lock);
            }
            boolean remove = refCount == 0;
            unlock(lock, lockOwner);
            // At this point, waiting threads *may* acquire the lock.  See acquireLock() for the check to reinstate
            // the lock in the CHM if the removal happens *after* the lock is acquired by a competing thread.
            getLog().tracef("Unlocking lock instance for key %s", key);

            // Ok, unlock was successful.  If the unlock was not successful, an exception will propagate and the entry will not be changed.
            return remove ? null : lock;
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
