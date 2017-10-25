package org.infinispan.util.concurrent.locks.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.ByRef;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
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
   private final ConcurrentMap<Object, InfinispanLock> lockMap;
   private Executor executor;
   private TimeService timeService;

   public PerKeyLockContainer() {
      lockMap = new ConcurrentHashMap<>(INITIAL_CAPACITY);
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR) Executor executor, TimeService timeService) {
      this.executor = executor;
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
      return new InfinispanLock(executor, timeService, () -> lockMap.computeIfPresent(key, (ignoredKey, lock) -> lock.isLocked() ? lock : null));
   }

}
