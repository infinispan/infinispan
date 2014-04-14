package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.VisibleOwnerReentrantLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A LockContainer that holds ReentrantLocks
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see OwnableReentrantStripedLockContainer
 * @since 4.0
 */
@ThreadSafe
public class ReentrantStripedLockContainer extends AbstractStripedLockContainer<VisibleOwnerReentrantLock> {

   private final VisibleOwnerReentrantLock[] sharedLocks;
   private static final Log log = LogFactory.getLog(ReentrantStripedLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   /**
    * Creates a new LockContainer which uses a certain number of shared locks across all elements that need to be
    * locked.
    *
    * @param concurrencyLevel concurrency level for number of stripes to create.  Stripes are created in powers of two,
    *                         with a minimum of concurrencyLevel created.
    */
   public ReentrantStripedLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(keyEquivalence);
      int numLocks = calculateNumberOfSegments(concurrencyLevel);
      sharedLocks = new VisibleOwnerReentrantLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new VisibleOwnerReentrantLock();
   }

   @Override
   public final VisibleOwnerReentrantLock getLock(Object object) {
      return sharedLocks[hashToIndex(object)];
   }

   @Override
   public final int getNumLocksHeld() {
      int i = 0;
      for (ReentrantLock l : sharedLocks)
         if (l.isLocked()) {
            i++;
         }
      return i;
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }

   @Override
   public final boolean ownsLock(Object object, Object ignored) {
      ReentrantLock lock = getLock(object);
      return lock.isHeldByCurrentThread();
   }

   @Override
   public final boolean isLocked(Object object) {
      ReentrantLock lock = getLock(object);
      return lock.isLocked();
   }

   @Override
   public String toString() {
      return "ReentrantStripedLockContainer{" +
            "sharedLocks=" + Arrays.toString(sharedLocks) +
            '}';
   }

   @Override
   protected void unlock(VisibleOwnerReentrantLock l, Object unused) {
      l.unlock();
   }

   @Override
   protected boolean tryLock(VisibleOwnerReentrantLock lock, long timeout, TimeUnit unit, Object unused) throws InterruptedException {
      return lock.tryLock(timeout, unit);
   }

   @Override
   protected void lock(VisibleOwnerReentrantLock lock, Object lockOwner) {
      lock.lock();
   }
}
