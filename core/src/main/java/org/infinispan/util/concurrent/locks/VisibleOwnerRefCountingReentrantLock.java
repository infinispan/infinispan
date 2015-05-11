package org.infinispan.util.concurrent.locks;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;

/**
 * A version of {@link VisibleOwnerReentrantLock} that has a reference counter, and implements {@link RefCountingLock}.
 * Used with a lock-per-entry container, in this case the {@link ReentrantPerEntryLockContainer}.
 *
 * @author Manik Surtani
 * @since 5.2
 * @see ReentrantPerEntryLockContainer
 */
public class VisibleOwnerRefCountingReentrantLock extends VisibleOwnerReentrantLock implements RefCountingLock {
   private final static AtomicIntegerFieldUpdater<VisibleOwnerRefCountingReentrantLock> updater
         = AtomicIntegerFieldUpdater.newUpdater(VisibleOwnerRefCountingReentrantLock.class, "references");

   private volatile int references = 1;

   @Override
   public int incrementRefCountAndGet() {
      return updater.incrementAndGet(this);
   }

   @Override
   public int decrementRefCountAndGet() {
      return updater.decrementAndGet(this);
   }


   @Override
   public String toString() {
      return super.toString() + "[References: "+references+"]";
   }
}
