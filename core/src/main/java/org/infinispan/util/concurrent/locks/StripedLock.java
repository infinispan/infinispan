package org.infinispan.util.concurrent.locks;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A simple implementation of lock striping, using cache entry keys to lock on, primarily used to help make {@link
 * org.infinispan.persistence.spi.CacheLoader} implemtations thread safe.
 * <p/>
 * Backed by a set of {@link java.util.concurrent.locks.ReentrantReadWriteLock} instances, and using the key hashcodes
 * to determine buckets.
 * <p/>
 * Since buckets are used, it doesn't matter that the key in question is not removed from the lock map when no longer in
 * use, since the key is not referenced in this class.  Rather, the hash code is used.
 * <p/>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ThreadSafe
public class StripedLock {

   private static final Log log = LogFactory.getLog(StripedLock.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final int DEFAULT_CONCURRENCY = 20;
   private final int lockSegmentMask;
   private final int lockSegmentShift;

   final ReentrantReadWriteLock[] sharedLocks;

   /**
    * This constructor just calls {@link #StripedLock(int)} with a default concurrency value of 20.
    */
   public StripedLock() {
      this(DEFAULT_CONCURRENCY);
   }

   /**
    * Creates a new StripedLock which uses a certain number of shared locks across all elements that need to be locked.
    *
    * @param concurrency number of threads expected to use this class concurrently.
    */
   public StripedLock(int concurrency) {
      int tempLockSegShift = 0;
      int numLocks = 1;
      while (numLocks < concurrency) {
         ++tempLockSegShift;
         numLocks <<= 1;
      }
      lockSegmentShift = 32 - tempLockSegShift;
      lockSegmentMask = numLocks - 1;

      sharedLocks = new ReentrantReadWriteLock[numLocks];

      for (int i = 0; i < numLocks; i++) {
        sharedLocks[i] = new ReentrantReadWriteLock();
    }
   }

   /**
    * Blocks until a lock is acquired.
    *
    * @param exclusive if true, a write (exclusive) lock is attempted, otherwise a read (shared) lock is used.
    */
   public void acquireLock(Object key, boolean exclusive) {
      ReentrantReadWriteLock lock = getLock(key);
      if (exclusive) {
         lock.writeLock().lock();
         if (trace) log.tracef("WL acquired for '%s'", key);
      } else {
         lock.readLock().lock();
         if (trace) log.tracef("RL acquired for '%s'", key);
      }
   }

   public boolean acquireLock(Object key, boolean exclusive, long millis) {
      ReentrantReadWriteLock lock = getLock(key);
      try {
         if (exclusive) {
            boolean success = lock.writeLock().tryLock(millis, TimeUnit.MILLISECONDS);
            if (success && trace) log.tracef("WL acquired for '%s'", key);
            return success;
         } else {
            boolean success = lock.readLock().tryLock(millis, TimeUnit.MILLISECONDS);
            if (success && trace) log.tracef("RL acquired for '%s'", key);
            return success;
         }
      } catch (InterruptedException e) {
         log.interruptedAcquiringLock(millis, e);
         return false;
      }
   }

   /**
    * Releases a lock the caller may be holding. This method is idempotent.
    */
   public void releaseLock(Object key) {
      ReentrantReadWriteLock lock = getLock(key);
      if (lock.isWriteLockedByCurrentThread()) {
         lock.writeLock().unlock();
         if (trace) log.tracef("WL released for '%s'", key);
      } else {
         lock.readLock().unlock();
         if (trace) log.tracef("RL released for '%s'", key);
      }
   }

   public void upgradeLock(Object key) {
      ReentrantReadWriteLock lock = getLock(key);
      lock.readLock().unlock();
      // another thread could come here and take the RL or WL, forcing us to wait
      lock.writeLock().lock();
      if (trace) log.tracef("RL upgraded to WL for '%s'", key);
   }

   public void downgradeLock(Object key) {
      ReentrantReadWriteLock lock = getLock(key);
      lock.readLock().lock();
      lock.writeLock().unlock();
      if (trace) log.tracef("WL downgraded to RL for '%s'", key);
   }

   final ReentrantReadWriteLock getLock(Object o) {
      return sharedLocks[hashToIndex(o)];
   }

   final int hashToIndex(Object o) {
      return hash(o) >>> lockSegmentShift & lockSegmentMask;
   }

   /**
    * Returns a hash code for non-null Object x. Uses the same hash code spreader as most other java.util hash tables,
    * except that this uses the string representation of the object passed in.
    *
    * @param x the object serving as a key
    * @return the hash code
    */
   static final int hash(Object x) {
      int h = x.hashCode();
      h ^= h >>> 20 ^ h >>> 12;
      return h ^ h >>> 7 ^ h >>> 4;
   }

   /**
    * Releases locks on all keys passed in.  Makes multiple calls to {@link #releaseLock(Object)}. This method is
    * idempotent.
    *
    * @param keys keys to unlock
    */
   public void releaseAllLocks(List<Object> keys) {
      for (Object k : keys) {
        releaseLock(k);
    }
   }

   /**
    * Acquires locks on keys passed in.  Makes multiple calls to {@link #acquireLock(Object, boolean)}
    *
    * @param keys      keys to unlock
    * @param exclusive whether locks are exclusive.
    */
   public void acquireAllLocks(List<Object> keys, boolean exclusive) {
      for (Object k : keys) {
        acquireLock(k, exclusive);
    }
   }

   /**
    * Returns the total number of locks held by this class.
    */
   public int getTotalLockCount() {
      int count = 0;
      for (ReentrantReadWriteLock lock : sharedLocks) {
         count += lock.getReadLockCount();
         count += lock.isWriteLocked() ? 1 : 0;
      }
      return count;
   }

   /**
    * Acquires RL on all locks aggregated by this StripedLock, in the given timeout.
    */
   public boolean acquireGlobalLock(boolean exclusive, long timeout) {
      log.tracef("About to acquire global lock. Exclusive? %s", exclusive);
      boolean success = true;
      for (int i = 0; i < sharedLocks.length; i++) {
         Lock toAcquire = exclusive ? sharedLocks[i].writeLock() : sharedLocks[i].readLock();
         try {
            success = toAcquire.tryLock(timeout, TimeUnit.MILLISECONDS);
            if (!success) {
               if (trace) log.tracef("Could not acquire lock on %s. Exclusive? %b", toAcquire, exclusive);
               break;
            }
         } catch (InterruptedException e) {
            if (trace) log.trace("Caught InterruptedException while trying to acquire global lock", e);
            success = false;
            Thread.currentThread().interrupt();
         } finally {
            if (!success) {
               for (int j = 0; j < i; j++) {
                  Lock toRelease = exclusive ? sharedLocks[j].writeLock() : sharedLocks[j].readLock();
                  toRelease.unlock();
               }
            }
         }
      }
      return success;
   }

   public void releaseGlobalLock(boolean exclusive) {
      for (ReentrantReadWriteLock lock : sharedLocks) {
         Lock toRelease = exclusive ? lock.writeLock() : lock.readLock();
         toRelease.unlock();
      }
   }

   public int getTotalReadLockCount() {
      int count = 0;
      for (ReentrantReadWriteLock lock : sharedLocks) {
         count += lock.getReadLockCount();
      }
      return count;
   }

   public int getSharedLockCount() {
      return sharedLocks.length;
   }

   public int getTotalWriteLockCount() {
      int count = 0;
      for (ReentrantReadWriteLock lock : sharedLocks) {
         count += lock.isWriteLocked() ? 1 : 0;
      }
      return count;
   }
}
