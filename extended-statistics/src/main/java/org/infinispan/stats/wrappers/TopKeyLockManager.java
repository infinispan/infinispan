package org.infinispan.stats.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.stats.topK.StreamSummaryContainer;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.Collection;

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
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      boolean isContented = isContented(key, ctx.getLockOwner());
      try {
         boolean lockAcquired = current.lockAndRecord(key, ctx, timeoutMillis);
         container.addLockInformation(key, isContented, !lockAcquired);
         return lockAcquired;
      } catch (InterruptedException e) {
         container.addLockInformation(key, isContented, true);
         throw e;
      } catch (RuntimeException e) {
         //TimeoutException extends RuntimeException!
         container.addLockInformation(key, isContented, true);
         throw e;
      }
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      current.unlock(lockedKeys, lockOwner);
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
   public boolean possiblyLocked(CacheEntry entry) {
      return current.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      return current.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      return current.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking)
         throws InterruptedException, TimeoutException {
      boolean isContented = isContented(key, ctx.getLockOwner());
      try {
         boolean retVal = current.acquireLock(ctx, key, timeoutMillis, skipLocking);
         container.addLockInformation(key, isContented, false);
         return retVal;
      } catch (InterruptedException e) {
         container.addLockInformation(key, isContented, true);
         throw e;
      } catch (RuntimeException e) {
         //TimeoutException extends RuntimeException!
         container.addLockInformation(key, isContented, true);
         throw e;
      }
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking)
         throws InterruptedException, TimeoutException {
      boolean isContented = isContented(key, ctx.getLockOwner());
      try {
         boolean retVal = current.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);
         container.addLockInformation(key, isContented, false);
         return retVal;
      } catch (InterruptedException e) {
         container.addLockInformation(key, isContented, true);
         throw e;
      } catch (RuntimeException e) {
         //TimeoutException extends RuntimeException!
         container.addLockInformation(key, isContented, true);
         throw e;
      }
   }

   private boolean isContented(Object key, Object requestor) {
      Object holder = current.getOwner(key);
      return holder != null && !holder.equals(requestor);
   }
}
