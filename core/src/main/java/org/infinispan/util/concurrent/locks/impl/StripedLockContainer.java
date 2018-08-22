package org.infinispan.util.concurrent.locks.impl;

import static org.infinispan.commons.util.InfinispanCollections.forEach;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.StripedHashFunction;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;

/**
 * A lock container used with lock stripping.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class StripedLockContainer implements LockContainer {

   private final InfinispanLock[] sharedLocks;
   private final StripedHashFunction<Object> hashFunction;

   public StripedLockContainer(int concurrencyLevel) {
      this.hashFunction = new StripedHashFunction<>(concurrencyLevel);
      sharedLocks = new InfinispanLock[hashFunction.getNumSegments()];
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR) Executor executor, TimeService timeService) {
      for (int i = 0; i < sharedLocks.length; i++) {
         if (sharedLocks[i] == null) {
            sharedLocks[i] = new InfinispanLock(executor, timeService);
         } else {
            sharedLocks[i].setTimeService(timeService);
         }
      }
   }

   @Override
   public ExtendedLockPromise acquire(Object key, Object lockOwner, long time, TimeUnit timeUnit) {
      return getLock(key).acquire(lockOwner, time, timeUnit);
   }

   @Override
   public void release(Object key, Object lockOwner) {
      getLock(key).release(lockOwner);
   }

   @Override
   public InfinispanLock getLock(Object key) {
      return sharedLocks[hashFunction.hashToSegment(key)];
   }

   @Override
   public int getNumLocksHeld() {
      int count = 0;
      for (InfinispanLock lock : sharedLocks) {
         if (lock.isLocked()) {
            count++;
         }
      }
      return count;
   }

   @Override
   public boolean isLocked(Object key) {
      return getLock(key).isLocked();
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }

   @Override
   public void deadlockCheck(DeadlockChecker deadlockChecker) {
      forEach(sharedLocks, lock -> lock.deadlockCheck(deadlockChecker));
   }

   @Override
   public String toString() {
      return "StripedLockContainer{" +
            "locks=" + Arrays.toString(sharedLocks) +
            '}';
   }
}
