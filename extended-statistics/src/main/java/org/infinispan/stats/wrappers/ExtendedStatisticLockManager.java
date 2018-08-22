package org.infinispan.stats.wrappers;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.LOCK_HOLD_TIME;
import static org.infinispan.stats.container.ExtendedStatistic.LOCK_WAITING_TIME;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_HELD_LOCKS;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_WAITED_FOR_LOCKS;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;

/**
 * Takes statistic about lock acquisition.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ExtendedStatisticLockManager implements LockManager {
   private final LockManager actual;
   private final CacheStatisticManager cacheStatisticManager;
   private final ConcurrentMap<Object, LockInfo> lockInfoMap = CollectionFactory.makeConcurrentMap();
   private final TimeService timeService;

   public ExtendedStatisticLockManager(LockManager actual, CacheStatisticManager cacheStatisticManager,
                                       TimeService timeService) {
      this.cacheStatisticManager = cacheStatisticManager;
      this.actual = actual;
      this.timeService = timeService;
   }

   public final LockManager getActual() {
      return actual;
   }

   @Override
   public KeyAwareLockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      if (lockOwnerAlreadyExists(key, lockOwner)) {
         return actual.lock(key, lockOwner, time, unit);
      }

      LockInfo lockInfo = new LockInfo(lockOwner instanceof GlobalTransaction ? (GlobalTransaction) lockOwner : null);
      updateContentionStats(key, lockInfo);

      final long start = timeService.time();
      final KeyAwareLockPromise lockPromise = actual.lock(key, lockOwner, time, unit);
      lockPromise.addListener((lockedKey, state) -> {
         long end = timeService.time();
         lockInfo.lockTimeStamp = end;
         if (lockInfo.contention) {
            lockInfo.lockWaiting = timeService.timeDuration(start, end, NANOSECONDS);
         }

         //if some owner tries to acquire the lock twice, we don't added it
         if (state == LockState.ACQUIRED) {
            lockInfoMap.putIfAbsent(lockedKey, lockInfo);
         } else {
            lockInfo.updateStats(null); //null == not locked
         }
      });
      return lockPromise;
   }

   @Override
   public KeyAwareLockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      if (keys.size() == 1) {
         return lock(keys.iterator().next(), lockOwner, time, unit);
      }
      final Map<Object, LockInfo> tmpMap = new HashMap<>();
      for (Object key : keys) {
         if (lockOwnerAlreadyExists(key, lockOwner)) {
            continue;
         }
         LockInfo lockInfo = new LockInfo(lockOwner instanceof GlobalTransaction ? (GlobalTransaction) lockOwner : null);
         updateContentionStats(key, lockInfo);
         tmpMap.put(key, lockInfo);
      }


      final long start = timeService.time();
      final KeyAwareLockPromise lockPromise = actual.lockAll(keys, lockOwner, time, unit);
      lockPromise.addListener((lockedKey, state) -> {
         long end = timeService.time();
         final LockInfo lockInfo = tmpMap.get(lockedKey);
         if (lockInfo == null) {
            return;
         }
         lockInfo.lockTimeStamp = end;
         if (lockInfo.contention) {
            lockInfo.lockWaiting = timeService.timeDuration(start, end, NANOSECONDS);
         }

         //if some owner tries to acquire the lock twice, we don't added it
         if (state == LockState.ACQUIRED) {
            lockInfoMap.putIfAbsent(lockedKey, lockInfo);
         } else {
            lockInfo.updateStats(null); //null == not locked
         }
      });
      return lockPromise;
   }

   @Override
   public void unlock(Object key, Object lockOwner) {
      final long timestamp = timeService.time();

      onUnlock(key, lockOwner, timestamp);
      actual.unlock(key, lockOwner);
   }

   @Override
   public void unlockAll(Collection<?> keys, Object lockOwner) {
      final long timestamp = timeService.time();

      for (Object key : keys) {
         onUnlock(key, lockOwner, timestamp);
      }
      actual.unlockAll(keys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      final long timestamp = timeService.time();
      final Object lockOwner = ctx.getLockOwner();

      for (Object key : ctx.getLockedKeys()) {
         onUnlock(key, lockOwner, timestamp);
      }
      actual.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      return actual.printLockInfo();
   }

   @Override
   public int getNumberOfLocksHeld() {
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public InfinispanLock getLock(Object key) {
      return actual.getLock(key);
   }

   private boolean lockOwnerAlreadyExists(Object key, Object lockOwner) {
      final InfinispanLock lock = actual.getLock(key);
      return lock != null && lock.containsLockOwner(lockOwner);
   }

   private void updateContentionStats(Object key, LockInfo lockInfo) {
      Object holder = getOwner(key);
      if (holder != null) {
         lockInfo.contention = !holder.equals(lockInfo.owner);
      }
   }

   private void onUnlock(Object key, Object lockOwner, long timestamp) {
      LockInfo lockInfo = lockInfoMap.get(key);
      if (lockInfo != null && lockInfo.owner.equals(lockOwner)) {
         lockInfo.updateStats(timestamp);
         lockInfoMap.remove(key);
      }
   }

   private class LockInfo {
      private final GlobalTransaction owner;
      private final boolean local;
      private long lockTimeStamp = -1;
      private boolean contention = false;
      private long lockWaiting = -1;

      public LockInfo(GlobalTransaction owner) {
         this.owner = owner;
         this.local = owner != null && !owner.isRemote();
      }

      public final void updateStats(Long releaseTimeStamp) {
         boolean locked = releaseTimeStamp != null;
         long holdTime = !locked ? 0 : timeService.timeDuration(lockTimeStamp, releaseTimeStamp, NANOSECONDS);
         cacheStatisticManager.add(LOCK_HOLD_TIME, holdTime, owner, local);
         if (lockWaiting != -1) {
            cacheStatisticManager.add(LOCK_WAITING_TIME, lockWaiting, owner, local);
            cacheStatisticManager.increment(NUM_WAITED_FOR_LOCKS, owner, local);
         }
         if (locked) {
            cacheStatisticManager.increment(NUM_HELD_LOCKS, owner, local);
         }
      }
   }
}
