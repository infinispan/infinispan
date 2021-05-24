package org.infinispan.util.concurrent.locks.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;

/**
 * A lock container that creates and maintains a new lock per entry.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public class PerKeyLockContainer implements LockContainer {

   private static final int INITIAL_CAPACITY = 32;
   private final ConcurrentMap<Object, InfinispanLock> lockMap;
   @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   @Inject protected Executor nonBlockingExecutor;
   private TimeService timeService;

   public PerKeyLockContainer() {
      lockMap = new ConcurrentHashMap<>(INITIAL_CAPACITY);
   }

   @Inject
   void inject(TimeService timeService) {
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
         return lock.isLocked() ? lock : null; //remove it if empty
      });
   }

   @Override
   public void releaseAll(Object lockOwner) {
      lockMap.forEach((key, lock) -> {
         if (lock.containsLockOwner(lockOwner)) {
            release(key, lockOwner);
         }
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
      return new InfinispanLock(nonBlockingExecutor, timeService, () -> lockMap.computeIfPresent(key, (ignoredKey, lock) -> lock.isLocked() ? lock : null));
   }

}
