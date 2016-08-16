package org.infinispan.util.concurrent.locks.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;

/**
 * A lock container that creates and maintains a new lock per entry.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class PerKeyLockContainer implements LockContainer {

   private static final int INITIAL_CAPACITY = 32;
   private final EquivalentConcurrentHashMapV8<Object, InfinispanLock> lockMap;
   private TimeService timeService;

   public PerKeyLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      lockMap = new EquivalentConcurrentHashMapV8<>(INITIAL_CAPACITY, concurrencyLevel, keyEquivalence, AnyEquivalence.getInstance());
   }

   @Inject
   public void inject(TimeService timeService) {
      this.timeService = timeService;
      for (InfinispanLock lock : lockMap.values()) {
         lock.setTimeService(timeService);
      }
   }

   @Override
   public ExtendedLockPromise acquire(Object key, Object lockOwner, long time, TimeUnit timeUnit) {
      ByRef<ExtendedLockPromise> reference = ByRef.create(null);
      lockMap.compute(key, (aKey, lock) -> {
         if (lock == null) {
            lock = createInfinispanLock(aKey);
         }
         reference.set(lock.acquire(lockOwner, time, timeUnit));
         return lock;
      });
      return reference.get();
   }

   @Override
   public InfinispanLock getLock(Object key) {
      return lockMap.get(key);
   }

   @Override
   public void release(Object key, Object lockOwner) {
      lockMap.computeIfPresent(key, (ignoredKey, lock) -> {
         lock.release(lockOwner);
         return !lock.isLocked() ? null : lock; //remove it if empty
      });
   }

   @Override
   public int getNumLocksHeld() {
      int count = 0;
      for (InfinispanLock lock : lockMap.values()) {
         if (lock.isLocked()) {
            count++;
         }
      }
      return count;
   }

   @Override
   public boolean isLocked(Object key) {
      InfinispanLock lock = lockMap.get(key);
      return lock != null && lock.isLocked();
   }

   @Override
   public int size() {
      return lockMap.size();
   }

   @Override
   public void deadlockCheck(DeadlockChecker deadlockChecker) {
      lockMap.values().forEach(lock -> lock.deadlockCheck(deadlockChecker));
   }

   @Override
   public String toString() {
      return "PerKeyLockContainer{" +
            "locks=" + lockMap +
            '}';
   }

   private InfinispanLock createInfinispanLock(Object key) {
      return new InfinispanLock(timeService, () -> lockMap.computeIfPresent(key, (ignoredKey, lock) -> lock.isLocked() ? lock : null));
   }

}
