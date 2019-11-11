package org.infinispan.query.impl.massindex;

import java.util.concurrent.Semaphore;

/**
 * A lock to prevent multiple {@link org.infinispan.query.MassIndexer} in non-clustered environments.
 * @since 10.1
 */
final class LocalMassIndexerLock implements MassIndexLock {

   private final Semaphore lock = new Semaphore(1);

   @Override
   public boolean lock() {
      return lock.tryAcquire();
   }

   @Override
   public void unlock() {
      lock.release();
   }

   @Override
   public boolean isAcquired() {
      return lock.availablePermits() == 1;
   }
}
