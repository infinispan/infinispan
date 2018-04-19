package org.infinispan.container.offheap;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.commons.util.ProcessorInfo;

/**
 * Holder for read write locks that provides ability to retrieve them by offset and hashCode
 * @author wburns
 * @since 9.0
 */
public class StripedLock {
   private static final int MAXIMUM_CAPACITY = 1 << 30;
   private static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash

   private final ReadWriteLock[] locks;

   public StripedLock() {
      this(ProcessorInfo.availableProcessors() * 2);
   }

   public StripedLock(int lockCount) {
      locks = new ReadWriteLock[tableSizeFor(lockCount)];
      for (int i = 0; i< locks.length; ++i) {
         locks[i] = new ReentrantReadWriteLock();
      }
   }

   /**
    * Retrieves the read write lock attributed to the given object using its hashCode for lookup.
    * @param obj the object to use to find the lock
    * @return the lock associated with the object
    */
   public ReadWriteLock getLock(Object obj) {
      return getLockFromHashCode(obj.hashCode());
   }

   /**
    * Retrieves the lock associated with the given hashCode
    * @param hashCode the hashCode to retrieve the lock for
    * @return the lock associated with the given hashCode
    */
   public ReadWriteLock getLockFromHashCode(int hashCode) {
      int h = spread(hashCode);
      int offset = h & (locks.length - 1);
      return locks[offset];
   }

   /**
    * Retrieves the given lock at a provided offset.  Note this is not hashCode based.  This method requires care
    * and the knowledge of how many locks there are.  This is useful when iterating over all locks
    * @param offset the offset of the lock to find
    * @return the lock at the given offset
    */
   public ReadWriteLock getLockWithOffset(int offset) {
      if (offset >= locks.length) {
         throw new ArrayIndexOutOfBoundsException();
      }
      return locks[offset];
   }

   /**
    * Locks all write locks.  Ensure that {@link StripedLock#unlockAll()} is called in a proper finally block
    */
   public void lockAll() {
      for (int i = 0; i < locks.length; ++i) {
         locks[i].writeLock().lock();
      }
   }

   /**
    * Unlocks all write locks, useful after {@link StripedLock#lockAll()} was invoked.
    */
   void unlockAll() {
      for (int i = 0; i < locks.length; ++i) {
         locks[i].writeLock().unlock();
      }
   }

   private static final int tableSizeFor(int c) {
      int n = c - 1;
      n |= n >>> 1;
      n |= n >>> 2;
      n |= n >>> 4;
      n |= n >>> 8;
      n |= n >>> 16;
      return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
   }

   static final int spread(int h) {
      return (h ^ (h >>> 16)) & HASH_BITS;
   }
}
