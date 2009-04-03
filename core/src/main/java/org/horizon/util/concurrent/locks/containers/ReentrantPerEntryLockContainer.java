package org.horizon.util.concurrent.locks.containers;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * // TODO: Manik: Document this!
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer {

   public ReentrantPerEntryLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   protected Lock newLock() {
      return new ReentrantLock();
   }

   public boolean ownsLock(Object key, Object owner) {
      ReentrantLock l = getLockFromMap(key);
      return l != null && l.isHeldByCurrentThread();
   }

   public boolean isLocked(Object key) {
      ReentrantLock l = getLockFromMap(key);
      return l != null && l.isLocked();
   }

   private ReentrantLock getLockFromMap(Object key) {
      return (ReentrantLock) locks.get(key);
   }
}
