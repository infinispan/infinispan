package org.infinispan.util.concurrent.locks;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;

/**
 * A version of {@link OwnableReentrantLock} that has a reference counter, and implements {@link RefCountingLock}.
 * Used with a lock-per-entry container, in this case the {@link OwnableReentrantPerEntryLockContainer}.
 *
 * @author Manik Surtani
 * @since 5.2
 * @see OwnableReentrantPerEntryLockContainer
 */
public class OwnableRefCountingReentrantLock extends OwnableReentrantLock implements RefCountingLock {
   private final static AtomicIntegerFieldUpdater<OwnableRefCountingReentrantLock> updater
         = AtomicIntegerFieldUpdater.newUpdater(OwnableRefCountingReentrantLock.class, "references");

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
