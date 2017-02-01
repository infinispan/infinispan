package org.infinispan.util.concurrent.locks.impl;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.infinispan.commons.util.Util.toStr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;
import org.infinispan.util.concurrent.locks.KeyAwareLockListener;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The default {@link LockManager} implementation for transactional and non-transactional caches.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@MBean(objectName = "LockManager", description = "Manager that handles MVCC locks for entries")
public class DefaultLockManager implements LockManager {

   private static final Log log = LogFactory.getLog(DefaultLockManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicReferenceFieldUpdater<CompositeLockPromise, LockState> UPDATER =
         newUpdater(CompositeLockPromise.class, LockState.class, "lockState");


   protected LockContainer lockContainer;
   protected Configuration configuration;
   protected ScheduledExecutorService scheduler;

   @Inject
   public void inject(LockContainer container, Configuration configuration,
                      @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService executorService) {
      this.lockContainer = container;
      this.configuration = configuration;
      this.scheduler = executorService;
   }


   @Override
   public KeyAwareLockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      Objects.requireNonNull(key, "Key must be non null");
      Objects.requireNonNull(lockOwner, "Lock owner must be non null");
      Objects.requireNonNull(unit, "Time unit must be non null");

      if (trace) {
         log.tracef("Lock key=%s for owner=%s. timeout=%s (%s)", toStr(key), lockOwner, time, unit);
      }

      ExtendedLockPromise promise = lockContainer.acquire(key, lockOwner, time, unit);
      return new KeyAwareExtendedLockPromise(promise, key, unit.toMillis(time)).scheduleLockTimeoutTask(scheduler);
   }

   @Override
   public KeyAwareLockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      Objects.requireNonNull(keys, "Keys must be non null");
      Objects.requireNonNull(lockOwner, "Lock owner must be non null");
      Objects.requireNonNull(unit, "Time unit must be non null");

      if (keys.isEmpty()) {
         if (trace) {
            log.tracef("Lock all: no keys found for owner=%s", lockOwner);
         }
         return KeyAwareLockPromise.NO_OP;
      } else if (keys.size() == 1) {
         //although will have the cost of creating an iterator, at least, we don't need to enter the synchronized section.
         return lock(keys.iterator().next(), lockOwner, time, unit);
      }

      final Set<Object> uniqueKeys = filterDistinctKeys(keys);

      if (uniqueKeys.size() == 1) {
         //although will have the cost of creating an iterator, at least, we don't need to enter the synchronized section.
         return lock(uniqueKeys.iterator().next(), lockOwner, time, unit);
      }

      if (trace) {
         log.tracef("Lock all keys=%s for owner=%s. timeout=%s (%s)", toStr(uniqueKeys), lockOwner, time,
               unit);
      }

      final CompositeLockPromise compositeLockPromise = new CompositeLockPromise(uniqueKeys.size());
      //needed to avoid internal deadlock when 2 or more lock owner invokes this method with the same keys.
      //ordering will not solve the problem since acquire() is non-blocking and each lock owner can iterate faster/slower than the other.
      synchronized (this) {
         for (Object key : uniqueKeys) {
            compositeLockPromise.addLock(new KeyAwareExtendedLockPromise(lockContainer.acquire(key, lockOwner, time, unit), key, unit.toMillis(time)));
         }
      }
      compositeLockPromise.markListAsFinal();
      return compositeLockPromise.scheduleLockTimeoutTask(scheduler, time, unit);
   }

   private Set<Object> filterDistinctKeys(Collection<?> collection) {
      if (collection instanceof Set) {
         //noinspection unchecked
         return (Set<Object>) collection;
      } else {
         return new HashSet<>(collection);
      }
   }

   @Override
   public void unlock(Object key, Object lockOwner) {
      if (trace) {
         log.tracef("Release lock for key=%s. owner=%s", key, lockOwner);
      }
      lockContainer.release(key, lockOwner);
   }

   @Override
   public void unlockAll(Collection<?> keys, Object lockOwner) {
      if (trace) {
         log.tracef("Release locks for keys=%s. owner=%s", toStr(keys), lockOwner);
      }
      if (keys.isEmpty()) {
         return;
      }
      for (Object key : keys) {
         lockContainer.release(key, lockOwner);
      }
   }

   @Override
   public void unlockAll(InvocationContext context) {
      unlockAll(context.getLockedKeys(), context.getLockOwner());
      context.clearLockedKeys();
   }

   @Override
   public boolean ownsLock(Object key, Object lockOwner) {
      Object currentOwner = getOwner(key);
      return currentOwner != null && currentOwner.equals(lockOwner);
   }

   @Override
   public boolean isLocked(Object key) {
      return getOwner(key) != null;
   }

   @Override
   public Object getOwner(Object key) {
      InfinispanLock lock = lockContainer.getLock(key);
      return lock == null ? null : lock.getLockOwner();
   }

   @Override
   public String printLockInfo() {
      return lockContainer.toString();
   }

   @Override
   @ManagedAttribute(description = "The number of exclusive locks that are held.", displayName = "Number of locks held")
   public int getNumberOfLocksHeld() {
      return lockContainer.getNumLocksHeld();
   }

   @ManagedAttribute(description = "The concurrency level that the MVCC Lock Manager has been configured with.", displayName = "Concurrency level", dataType = DataType.TRAIT)
   public int getConcurrencyLevel() {
      return configuration.locking().concurrencyLevel();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are available.", displayName = "Number of locks available")
   public int getNumberOfLocksAvailable() {
      return lockContainer.size() - lockContainer.getNumLocksHeld();
   }

   @Override
   public InfinispanLock getLock(Object key) {
      return lockContainer.getLock(key);
   }

   private static class KeyAwareExtendedLockPromise implements KeyAwareLockPromise, ExtendedLockPromise, Callable<Void> {

      private final ExtendedLockPromise lockPromise;
      private final Object key;
      private final long timeoutMillis;

      private KeyAwareExtendedLockPromise(ExtendedLockPromise lockPromise, Object key, long timeoutMillis) {
         this.lockPromise = lockPromise;
         this.key = key;
         this.timeoutMillis = timeoutMillis;
      }

      @Override
      public void cancel(LockState cause) {
         lockPromise.cancel(cause);
      }

      @Override
      public Object getRequestor() {
         return lockPromise.getRequestor();
      }

      @Override
      public Object getOwner() {
         return lockPromise.getOwner();
      }

      @Override
      public boolean isAvailable() {
         return lockPromise.isAvailable();
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {
         try {
            lockPromise.lock();
         } catch (TimeoutException e) {
            throw log.unableToAcquireLock(Util.prettyPrintTime(timeoutMillis), toStr(key),
                  lockPromise.getRequestor(), lockPromise.getOwner());
         }
      }

      @Override
      public void addListener(LockListener listener) {
         lockPromise.addListener(listener);
      }

      @Override
      public void addListener(KeyAwareLockListener listener) {
         lockPromise.addListener(state -> listener.onEvent(key, state));
      }

      @Override
      public Void call() throws Exception{
         lockPromise.cancel(LockState.TIMED_OUT);
         return null;
      }

      KeyAwareExtendedLockPromise scheduleLockTimeoutTask(ScheduledExecutorService executorService) {
         if (executorService != null && timeoutMillis > 0 && !isAvailable()) {
            ScheduledFuture<?> future = executorService.schedule(this, timeoutMillis, TimeUnit.MILLISECONDS);
            lockPromise.addListener((state -> future.cancel(false)));
         }
         return this;
      }
   }

   private static class CompositeLockPromise implements KeyAwareLockPromise, LockListener, Callable<Void> {

      private final List<KeyAwareExtendedLockPromise> lockPromiseList;
      private final CompletableFuture<LockState> notifier;
      volatile LockState lockState = LockState.ACQUIRED;
      private final AtomicInteger countersLeft = new AtomicInteger();

      private CompositeLockPromise(int size) {
         lockPromiseList = new ArrayList<>(size);
         notifier = new CompletableFuture<>();
      }

      void addLock(KeyAwareExtendedLockPromise lockPromise) {
         lockPromiseList.add(lockPromise);
      }

      void markListAsFinal() {
         countersLeft.set(lockPromiseList.size());
         for (LockPromise lockPromise : lockPromiseList) {
            lockPromise.addListener(this);
         }
      }

      @Override
      public boolean isAvailable() {
         return notifier.isDone();
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {
         InterruptedException interruptedException = null;
         TimeoutException timeoutException = null;
         DeadlockDetectedException deadlockException = null;
         RuntimeException runtimeException = null;
         for (ExtendedLockPromise lockPromise : lockPromiseList) {
            try {
               //we still need to invoke lock in all the locks.
               lockPromise.lock();
            } catch (InterruptedException e) {
               interruptedException = e;
            } catch (TimeoutException e) {
               timeoutException = e;
            } catch (DeadlockDetectedException e) {
               deadlockException = e;
            } catch (RuntimeException e) {
               runtimeException = e;
            }
         }
         if (interruptedException != null) {
            throw interruptedException;
         } else if (timeoutException != null) {
            throw timeoutException;
         } else if (deadlockException != null) {
            throw deadlockException;
         } else if (runtimeException != null) {
            throw runtimeException;
         }
      }

      @Override
      public void addListener(LockListener listener) {
         notifier.thenAccept(listener::onEvent);
      }

      @Override
      public void onEvent(LockState state) {
         if (notifier.isDone()) {
            //already finished
            return;
         }
         //each lock will invoke this
         if (state != LockState.ACQUIRED) {
            cancelAll(state);
            return;
         }
         if (countersLeft.decrementAndGet() == 0) {
            notifier.complete(lockState);
         }
      }

      private void cancelAll(LockState state) {
         if (UPDATER.compareAndSet(this, LockState.ACQUIRED, state)) {
            //complete the future before cancel other locks. the remaining locks will be invoke onEvent()
            notifier.complete(state);
            lockPromiseList.forEach(promise -> promise.cancel(state));
         }
      }

      @Override
      public void addListener(KeyAwareLockListener listener) {
         for (KeyAwareExtendedLockPromise lockPromise : lockPromiseList) {
            lockPromise.addListener(listener);
         }
      }

      @Override
      public Void call() throws Exception {
         lockPromiseList.forEach(promise -> promise.cancel(LockState.TIMED_OUT));
         return null;
      }

      CompositeLockPromise scheduleLockTimeoutTask(ScheduledExecutorService executorService, long time, TimeUnit unit) {
         if (executorService != null && time > 0 && !isAvailable()) {
            ScheduledFuture<?> future = executorService.schedule(this, time, unit);
            addListener((state -> future.cancel(false)));
         }
         return this;
      }
   }
}
