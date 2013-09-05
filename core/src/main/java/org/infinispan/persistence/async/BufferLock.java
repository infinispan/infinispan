package org.infinispan.persistence.async;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * A custom reader-writer-lock combined with a bounded buffer size counter.
 * <p/>
 * Supports multiple concurrent writers and a single exclusive reader. This ensures that no more
 * data is being written to the current state when the AsyncStoreCoordinator thread hands the
 * data off to the back-end store.
 * <p/>
 * Additionally, {@link #writeLock(int)} blocks if the buffer is full, and {@link #readLock()}
 * blocks if no data is available.
 * <p/>
 * This lock implementation is <em>not</em> reentrant!
 *
 *  @author Karsten Blees
 */
class BufferLock {
   /**
    * AQS state is the number of 'items' in the buffer. AcquireShared blocks if the buffer is
    * full (>= size).
    */
   private static class Counter extends AbstractQueuedSynchronizer {
      private static final long serialVersionUID = 1688655561670368887L;
      private final int size;

      Counter(int size) {
         this.size = size;
      }

      int add(int count) {
         for (;;) {
            int state = getState();
            if (compareAndSetState(state, state + count))
               return state + count;
         }
      }

      @Override
      protected int tryAcquireShared(int count) {
         for (;;) {
            int state = getState();
            if (state >= size)
               return -1;
            if (compareAndSetState(state, state + count))
               return state + count >= size ? 0 : 1;
         }
      }

      @Override
      protected boolean tryReleaseShared(int state) {
         setState(state);
         return state < size;
      }
   }

   /**
    * AQS state is 0 if no data is available, 1 otherwise. AcquireShared blocks if no data is
    * available.
    */
   private static class Available extends AbstractQueuedSynchronizer {
      private static final long serialVersionUID = 6464514100313353749L;

      @Override
      protected int tryAcquireShared(int unused) {
         return getState() > 0 ? 1 : -1;
      }

      @Override
      protected boolean tryReleaseShared(int state) {
         setState(state > 0 ? 1 : 0);
         return state > 0;
      }
   }

   /**
    * Minimal non-reentrant read-write-lock. AQS state is number of concurrent shared locks, or 0
    * if unlocked, or -1 if locked exclusively.
    */
   private static class Sync extends AbstractQueuedSynchronizer {
      private static final long serialVersionUID = 2983687000985096017L;

      @Override
      protected boolean tryAcquire(int unused) {
         if (!compareAndSetState(0, -1))
            return false;
         setExclusiveOwnerThread(Thread.currentThread());
         return true;
      }

      @Override
      protected boolean tryRelease(int unused) {
         setExclusiveOwnerThread(null);
         setState(0);
         return true;
      }

      @Override
      protected int tryAcquireShared(int unused) {
         for (;;) {
            int state = getState();
            if (state < 0)
               return -1;
            if (compareAndSetState(state, state + 1))
               return 1;
         }
      }

      @Override
      protected boolean tryReleaseShared(int unused) {
         for (;;) {
            int state = getState();
            if (compareAndSetState(state, state - 1))
               return true;
         }
      }
   }

   private final Sync sync;
   private final Counter counter;
   private final Available available;

   /**
    * Create a new BufferLock with the specified buffer size.
    *
    * @param size
    *           the buffer size
    */
   BufferLock(int size) {
      sync = new Sync();
      counter = size > 0 ? new Counter(size) : null;
      available = new Available();
   }

   /**
    * Acquires the write lock and consumes the specified amount of buffer space. Blocks if the
    * buffer is full or if the object is currently locked for reading.
    *
    * @param count
    *           number of items the caller intends to write
    */
   void writeLock(int count) {
      if (counter != null)
         counter.acquireShared(count);
      sync.acquireShared(1);
   }

   /**
    * Releases the write lock.
    */
   void writeUnlock() {
      sync.releaseShared(1);
      available.releaseShared(1);
   }

   /**
    * Acquires the read lock. Blocks if the buffer is empty or if the object is currently locked
    * for writing.
    */
   void readLock() {
      available.acquireShared(1);
      sync.acquire(1);
   }

   /**
    * Releases the read lock.
    */
   void readUnlock() {
      sync.release(1);
   }

   /**
    * Resets the buffer counter to the specified number.
    *
    * @param count
    *           number of available items in the buffer
    */
   void reset(int count) {
      if (counter != null)
         counter.releaseShared(count);
      available.releaseShared(count);
   }

   /**
    * Modifies the buffer counter by the specified value.
    *
    * @param count
    *           number of items to add to the buffer counter
    */
   void add(int count) {
      if (counter != null)
         count = counter.add(count);
      available.releaseShared(count);
   }
}
