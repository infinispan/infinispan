package org.infinispan.stats.wrappers;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.context.InvocationContext;
import org.infinispan.stats.topK.StreamSummaryContainer;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;

/**
 * Top-key stats about locks.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class TopKeyLockManager implements LockManager {

   private final LockManager current;
   private final StreamSummaryContainer container;

   public TopKeyLockManager(LockManager current, StreamSummaryContainer container) {
      this.current = current;
      this.container = container;
   }

   @Override
   public KeyAwareLockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      if (lockOwnerAlreadyExists(key, lockOwner)) {
         return current.lock(key, lockOwner, time, unit);
      }
      KeyAwareLockPromise lockPromise = current.lock(key, lockOwner, time, unit);
      final boolean contented = !lockOwner.equals(current.getOwner(key));
      lockPromise.addListener(state -> container.addLockInformation(key, contented, state != LockState.ACQUIRED));
      return lockPromise;
   }

   @Override
   public KeyAwareLockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      final Set<Object> keysToTrack = keys.stream().filter(key -> !lockOwnerAlreadyExists(key, lockOwner)).collect(Collectors.toSet());
      final KeyAwareLockPromise lockPromise = current.lockAll(keys, lockOwner, time, unit);
      final Set<Object> contentedKeys = keys.stream().filter(key -> !lockOwner.equals(current.getOwner(key))).collect(Collectors.toSet());
      lockPromise.addListener((lockedKey, state) -> {
         if (keysToTrack.contains(lockedKey)) {
            container.addLockInformation(lockedKey, contentedKeys.contains(lockedKey), state != LockState.ACQUIRED);
         }
      });
      return lockPromise;
   }

   @Override
   public void unlock(Object key, Object lockOwner) {
      current.unlock(key, lockOwner);
   }

   @Override
   public void unlockAll(Collection<?> keys, Object lockOwner) {
      current.unlockAll(keys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      current.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return current.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return current.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      return current.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      return current.printLockInfo();
   }

   @Override
   public int getNumberOfLocksHeld() {
      return current.getNumberOfLocksHeld();
   }

   @Override
   public InfinispanLock getLock(Object key) {
      return current.getLock(key);
   }

   private boolean lockOwnerAlreadyExists(Object key, Object lockOwner) {
      final InfinispanLock lock = current.getLock(key);
      return lock != null && lock.containsLockOwner(lockOwner);
   }
}
