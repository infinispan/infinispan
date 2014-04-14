package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.OwnableReentrantLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A LockContainer that holds {@link org.infinispan.util.concurrent.locks.OwnableReentrantLock}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see ReentrantStripedLockContainer
 * @see org.infinispan.util.concurrent.locks.OwnableReentrantLock
 * @since 4.0
 */
@ThreadSafe
public class OwnableReentrantStripedLockContainer extends AbstractStripedLockContainer<OwnableReentrantLock> {

   private final OwnableReentrantLock[] sharedLocks;
   private static final Log log = LogFactory.getLog(OwnableReentrantStripedLockContainer.class);

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
   public OwnableReentrantStripedLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(keyEquivalence);
      int numLocks = calculateNumberOfSegments(concurrencyLevel);
      sharedLocks = new OwnableReentrantLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new OwnableReentrantLock();
   }

   @Override
   public final OwnableReentrantLock getLock(Object object) {
      return sharedLocks[hashToIndex(object)];
   }

   @Override
   public final boolean ownsLock(Object object, Object owner) {
      OwnableReentrantLock lock = getLock(object);
      return owner.equals(lock.getOwner());
   }

   @Override
   public final boolean isLocked(Object object) {
      OwnableReentrantLock lock = getLock(object);
      return lock.isLocked();
   }

   @Override
   public final int getNumLocksHeld() {
      int i = 0;
      for (OwnableReentrantLock l : sharedLocks) if (l.isLocked()) i++;
      return i;
   }

   @Override
   public String toString() {
      return "OwnableReentrantStripedLockContainer{" +
            "sharedLocks=" + Arrays.toString(sharedLocks) +
            '}';
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }

   @Override
   protected boolean tryLock(OwnableReentrantLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected void lock(OwnableReentrantLock lock, Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected void unlock(OwnableReentrantLock l, Object owner) {
      l.unlock(owner);
   }
}
