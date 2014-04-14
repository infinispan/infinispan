package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.OwnableReentrantLock;
import org.infinispan.util.concurrent.locks.OwnableRefCountingReentrantLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * A per-entry lock container for OwnableReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class OwnableReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<OwnableRefCountingReentrantLock> {

   private static final Log log = LogFactory.getLog(OwnableReentrantPerEntryLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public OwnableReentrantPerEntryLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(concurrencyLevel, keyEquivalence);
   }

   @Override
   protected OwnableRefCountingReentrantLock newLock() {
      return new OwnableRefCountingReentrantLock();
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      OwnableReentrantLock l = getLockFromMap(key);
      return l != null && owner.equals(l.getOwner());
   }

   @Override
   public boolean isLocked(Object key) {
      OwnableReentrantLock l = getLockFromMap(key);
      return l != null && l.isLocked();
   }

   private OwnableReentrantLock getLockFromMap(Object key) {
      return locks.get(key);
   }

   @Override
   protected boolean tryLock(OwnableRefCountingReentrantLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected void lock(OwnableRefCountingReentrantLock lock, Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected void unlock(OwnableRefCountingReentrantLock l, Object owner) {
      l.unlock(owner);
   }
}
