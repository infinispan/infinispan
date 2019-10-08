package org.infinispan.container.offheap;

import java.util.concurrent.locks.StampedLock;

import org.infinispan.commons.util.Util;

/**
 * Holder for stamped locks that provides ability to retrieve them by offset and hashCode
 * Note that locks protect entries
 * @author wburns
 * @since 9.0
 */
public class StripedLock {
   private final StampedLock[] locks;

   public StripedLock(int lockCount) {
      locks = new StampedLock[Util.findNextHighestPowerOfTwo(lockCount)];
      for (int i = 0; i< locks.length; ++i) {
         locks[i] = new StampedLock();
      }
   }

   /**
    * Retrieves the given lock at a provided offset.  Note this is not hashCode based.  This method requires care
    * and the knowledge of how many locks there are.  This is useful when iterating over all locks
    * @param offset the offset of the lock to find
    * @return the lock at the given offset
    */
   public StampedLock getLockWithOffset(int offset) {
      if (offset >= locks.length) {
         throw new ArrayIndexOutOfBoundsException();
      }
      return locks[offset];
   }

   /**
    * Locks all write locks.  Ensure that {@link StripedLock#unlockAll()} is called in a proper finally block
    */
   public void lockAll() {
      for (StampedLock rwLock : locks) {
         rwLock.asWriteLock().lock();
      }
   }

   /**
    * Unlocks all write locks, useful after {@link StripedLock#lockAll()} was invoked.
    */
   void unlockAll() {
      for (StampedLock rwLock : locks) {
         rwLock.asWriteLock().unlock();
      }
   }
}
