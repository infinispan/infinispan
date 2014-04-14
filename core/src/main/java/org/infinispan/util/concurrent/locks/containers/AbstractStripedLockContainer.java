package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.equivalence.Equivalence;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A container for locks.  Used with lock striping.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@ThreadSafe
public abstract class AbstractStripedLockContainer<L extends Lock> extends AbstractLockContainer<L> {
   private int lockSegmentMask;
   private int lockSegmentShift;
   private final Equivalence<Object> keyEquivalence;

   protected AbstractStripedLockContainer(Equivalence<Object> keyEquivalence) {
      this.keyEquivalence = keyEquivalence;
   }

   final int calculateNumberOfSegments(int concurrencyLevel) {
      int tempLockSegShift = 0;
      int numLocks = 1;
      while (numLocks < concurrencyLevel) {
         ++tempLockSegShift;
         numLocks <<= 1;
      }
      lockSegmentShift = 32 - tempLockSegShift;
      lockSegmentMask = numLocks - 1;
      return numLocks;
   }

   final int hashToIndex(Object object) {
      return (hash(keyEquivalence.hashCode(object)) >>> lockSegmentShift) & lockSegmentMask;
   }

   /**
    * Returns a hash code for non-null Object x. Uses the same hash code spreader as most other java.util hash tables,
    * except that this uses the string representation of the object passed in.
    *
    * @param hashCode the object's hash code serving as a key.
    * @return the hash code
    */
   static int hash(int hashCode) {
      int h = hashCode;
      h += ~(h << 9);
      h ^= (h >>> 14);
      h += (h << 4);
      h ^= (h >>> 10);
      return h;

   }

   @Override
   public L acquireLock(Object lockOwner, Object key, long timeout, TimeUnit unit) throws InterruptedException {
      L lock = getLock(key);
      boolean locked;
      try {
         locked = tryLock(lock, timeout, unit, lockOwner);
      } catch (InterruptedException ie) {
         safeRelease(lock, lockOwner);
         throw ie;
      } catch (Throwable th) {
         safeRelease(lock, lockOwner);
         locked = false;
      }
      return locked ? lock : null;
   }

   @Override
   public void releaseLock(Object lockOwner, Object key) {
      final L lock = getLock(key);
      safeRelease(lock, lockOwner);
   }

   @Override
   public int getLockId(Object key) {
      return hashToIndex(key);
   }
}
