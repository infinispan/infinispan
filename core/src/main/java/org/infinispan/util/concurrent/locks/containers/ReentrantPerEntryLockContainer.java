package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.util.concurrent.locks.VisibleOwnerRefCountingReentrantLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A per-entry lock container for ReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<VisibleOwnerRefCountingReentrantLock> {

   private static final Log log = LogFactory.getLog(ReentrantPerEntryLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public ReentrantPerEntryLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   protected VisibleOwnerRefCountingReentrantLock newLock() {
      return new VisibleOwnerRefCountingReentrantLock();
   }

   @Override
   public boolean ownsLock(Object key, Object ignored) {
      ReentrantLock l = getLockFromMap(key);
      return l != null && l.isHeldByCurrentThread();
   }

   @Override
   public boolean isLocked(Object key) {
      ReentrantLock l = getLockFromMap(key);
      return l != null && l.isLocked();
   }

   private ReentrantLock getLockFromMap(Object key) {
      return locks.get(key);
   }

   @Override
   protected void unlock(VisibleOwnerRefCountingReentrantLock l, Object unused) {
      l.unlock();
   }

   @Override
   protected boolean tryLock(VisibleOwnerRefCountingReentrantLock lock, long timeout, TimeUnit unit, Object unused) throws InterruptedException {
      return lock.tryLock(timeout, unit);
   }

   @Override
   protected void lock(VisibleOwnerRefCountingReentrantLock lock, Object lockOwner) {
      lock.lock();
   }
}
