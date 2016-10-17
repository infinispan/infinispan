package org.infinispan.container.offheap;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author wburns
 * @since 9.0
 */
public class StripedLock {
   private static final int MAXIMUM_CAPACITY = 1 << 30;
   private static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash

   private final ReadWriteLock[] locks;

   public StripedLock() {
      this(Runtime.getRuntime().availableProcessors() * 2);
   }

   public StripedLock(int lockCount) {
      locks = new ReadWriteLock[tableSizeFor(lockCount)];
      for (int i = 0; i< locks.length; ++i) {
         locks[i] = new ReentrantReadWriteLock();
      }
   }

   public ReadWriteLock getLock(Object obj) {
      int h = spread(obj.hashCode());
      int offset = h & (locks.length - 1);
      return locks[offset];
   }

   public ReadWriteLock getLockWithOffset(int offset) {
      if (offset >= locks.length) {
         throw new ArrayIndexOutOfBoundsException();
      }
      return locks[offset];
   }

   void lockAll() {
      for (int i = 0; i < locks.length; ++i) {
         locks[i].writeLock().lock();
      }
   }

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
