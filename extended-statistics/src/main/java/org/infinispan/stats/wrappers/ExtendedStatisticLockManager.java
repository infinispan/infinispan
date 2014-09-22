package org.infinispan.stats.wrappers;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;

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
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      return actual.lockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      final long timestamp = timeService.time();

      for (Object key : lockedKeys) {
         LockInfo lockInfo = lockInfoMap.get(key);
         if (lockInfo != null && lockInfo.owner.equals(lockOwner)) {
            lockInfo.updateStats(timestamp);
            lockInfoMap.remove(key);
         }
      }
      actual.unlock(lockedKeys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      List<LockInfo> acquiredLockInfo = new ArrayList<LockInfo>();
      for (Object key : ctx.getLockedKeys()) {
         LockInfo lockInfo = lockInfoMap.get(key);
         if (lockInfo != null && lockInfo.owner.equals(ctx.getLockOwner())) {
            acquiredLockInfo.add(lockInfo);
            lockInfoMap.remove(key);
         }
      }
      actual.unlockAll(ctx);
      long timestamp = timeService.time();
      for (LockInfo lockInfo : acquiredLockInfo) {
         lockInfo.updateStats(timestamp);
      }
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
   public boolean possiblyLocked(CacheEntry entry) {
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      return actual.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      LockInfo lockInfo = new LockInfo(ctx);
      updateContentionStats(key, lockInfo);

      boolean locked = false;
      long start = timeService.time();
      try {
         locked = actual.acquireLock(ctx, key, timeoutMillis, skipLocking);  //this returns false if you already have acquired the lock previously
      } finally {
         long end = timeService.time();
         lockInfo.lockTimeStamp = end;
         if (lockInfo.contention) {
            lockInfo.lockWaiting = timeService.timeDuration(start, end, NANOSECONDS);
         }

         //if some owner tries to acquire the lock twice, we don't added it
         if (locked) {
            lockInfoMap.putIfAbsent(key, lockInfo);
         } else {
            lockInfo.updateStats(null); //null == not locked
         }
      }

      return locked;
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      LockInfo lockInfo = new LockInfo(ctx);
      updateContentionStats(key, lockInfo);

      boolean locked = false;
      long start = timeService.time();
      try {
         locked = actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);  //this returns false if you already have acquired the lock previously
      } finally {
         long end = timeService.time();
         lockInfo.lockTimeStamp = end;
         if (lockInfo.contention) {
            lockInfo.lockWaiting = timeService.timeDuration(start, end, NANOSECONDS);
         }

         //if some owner tries to acquire the lock twice, we don't added it
         if (locked) {
            lockInfoMap.putIfAbsent(key, lockInfo);
         } else {
            lockInfo.updateStats(null); //null == not locked
         }
      }

      return locked;
   }

   private void updateContentionStats(Object key, LockInfo lockInfo) {
      Object holder = getOwner(key);
      if (holder != null) {
         lockInfo.contention = !holder.equals(lockInfo.owner);
      }
   }

   private class LockInfo {
      private final GlobalTransaction owner;
      private final boolean local;
      private long lockTimeStamp = -1;
      private boolean contention = false;
      private long lockWaiting = -1;

      public LockInfo(InvocationContext ctx) {
         owner = ctx.getLockOwner() instanceof GlobalTransaction ? (GlobalTransaction) ctx.getLockOwner() : null;
         local = ctx.isOriginLocal();
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
