package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A version of {@link VisibleOwnerReentrantLock} that has a reference counter, and implements {@link RefCountingLock}.
 * Used with a lock-per-entry container, in this case the {@link ReentrantPerEntryLockContainer}.
 *
 * @author Manik Surtani
 * @since 5.2
 * @see ReentrantPerEntryLockContainer
 */
public class VisibleOwnerRefCountingReentrantLock extends VisibleOwnerReentrantLock implements RefCountingLock {
   private final AtomicInteger references = new AtomicInteger(1);

   @Override
   public AtomicInteger getReferenceCounter() {
      return references;
   }

   @Override
   public String toString() {
      return super.toString() + "[References: "+references.toString()+"]";
   }
}
