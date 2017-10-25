package org.infinispan.util.concurrent.locks.impl;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.infinispan.util.concurrent.CompletableFutures.await;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.SyncInvocationStage;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A special lock for Infinispan cache.
 * <p/>
 * The main different with the traditional {@link java.util.concurrent.locks.Lock} is allowing to use any object as lock
 * owner. It is possible to use a {@link Thread} as lock owner that makes similar to {@link
 * java.util.concurrent.locks.Lock}.
 * <p/>
 * In addition, it has an asynchronous interface. {@link #acquire(Object, long, TimeUnit)}  will not acquire the lock
 * immediately (except if it is free) but will return a {@link ExtendedLockPromise}. This promise allow to test if the
 * lock is acquired asynchronously and cancel the lock acquisition, without any blocking.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class InfinispanLock {

   private static final Log log = LogFactory.getLog(InfinispanLock.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicReferenceFieldUpdater<InfinispanLock, LockPlaceHolder> OWNER_UPDATER =
         newUpdater(InfinispanLock.class, LockPlaceHolder.class, "current");
   private static final AtomicReferenceFieldUpdater<LockPlaceHolder, LockState> STATE_UPDATER =
         newUpdater(LockPlaceHolder.class, LockState.class, "lockState");


   private final Queue<LockPlaceHolder> pendingRequest;
   private final ConcurrentMap<Object, LockPlaceHolder> lockOwners;
   private final Runnable releaseRunnable;
   private final Executor executor;
   private TimeService timeService;
   @SuppressWarnings("CanBeFinal")
   private volatile LockPlaceHolder current;

   /**
    * Creates a new instance.
    *
    * @param executor
    * @param timeService the {@link TimeService} to check for timeouts.
    */
   public InfinispanLock(Executor executor, TimeService timeService) {
      this.executor = executor;
      this.timeService = timeService;
      pendingRequest = new ConcurrentLinkedQueue<>();
      lockOwners = new ConcurrentHashMap<>();
      current = null;
      releaseRunnable = null;
   }

   /**
    * Creates a new instance.
    *
    * @param executor
    * @param timeService     the {@link TimeService} to check for timeouts.
    * @param releaseRunnable a {@link Runnable} that is invoked every time this lock is released.
    */
   public InfinispanLock(Executor executor, TimeService timeService, Runnable releaseRunnable) {
      this.executor = executor;
      this.timeService = timeService;
      pendingRequest = new ConcurrentLinkedQueue<>();
      lockOwners = new ConcurrentHashMap<>();
      current = null;
      this.releaseRunnable = releaseRunnable;
   }

   /**
    * Tests purpose only!
    */
   public void setTimeService(TimeService timeService) {
      if (timeService != null) {
         this.timeService = timeService;
      }
   }

   /**
    * It tries to acquire this lock.
    * <p/>
    * If it is invoked multiple times with the same owner, the same {@link ExtendedLockPromise} is returned until it has
    * timed-out or {@link #release(Object)}  is invoked.
    * <p/>
    * If the lock is free, it is immediately acquired, otherwise the lock owner is queued.
    *
    * @param lockOwner the lock owner who needs to acquire the lock.
    * @param time      the timeout value.
    * @param timeUnit  the timeout unit.
    * @return an {@link ExtendedLockPromise}.
    * @throws NullPointerException if {@code lockOwner} or {@code timeUnit} is {@code null}.
    */
   public ExtendedLockPromise acquire(Object lockOwner, long time, TimeUnit timeUnit) {
      Objects.requireNonNull(lockOwner, "Lock Owner should be non-null");
      Objects.requireNonNull(timeUnit, "Time Unit should be non-null");

      if (trace) {
         log.tracef("Acquire lock for %s. Timeout=%s (%s)", lockOwner, time, timeUnit);
      }

      LockPlaceHolder lockPlaceHolder = lockOwners.get(lockOwner);
      if (lockPlaceHolder != null) {
         if (trace) {
            log.tracef("Lock owner already exists: %s", lockPlaceHolder);
         }
         return lockPlaceHolder;
      }

      lockPlaceHolder = createLockInfo(lockOwner, time, timeUnit);
      LockPlaceHolder other = lockOwners.putIfAbsent(lockOwner, lockPlaceHolder);

      if (other != null) {
         if (trace) {
            log.tracef("Lock owner already exists: %s", other);
         }
         return other;
      }

      if (trace) {
         log.tracef("Created a new one: %s", lockPlaceHolder);
      }

      pendingRequest.add(lockPlaceHolder);
      tryAcquire(null);
      return lockPlaceHolder;
   }

   /**
    * It tries to release the lock held by {@code lockOwner}.
    * <p/>
    * If the lock is not acquired (is waiting or timed out/deadlocked) by {@code lockOwner}, its {@link
    * ExtendedLockPromise} is canceled. If {@code lockOwner} is the current lock owner, the lock is released and the
    * next lock owner available will acquire the lock. If the {@code lockOwner} never tried to acquire the lock, this
    * method does nothing.
    *
    * @param lockOwner the lock owner who wants to release the lock.
    * @throws NullPointerException if {@code lockOwner} is {@code null}.
    */
   public void release(Object lockOwner) {
      Objects.requireNonNull(lockOwner, "Lock Owner should be non-null");

      if (trace) {
         log.tracef("Release lock for %s.", lockOwner);
      }

      LockPlaceHolder wantToRelease = lockOwners.get(lockOwner);
      if (wantToRelease == null) {
         if (trace) {
            log.tracef("%s not found!", lockOwner);
         }
         //nothing to release
         return;
      }

      final boolean released = wantToRelease.setReleased();
      if (trace) {
         log.tracef("Release lock for %s? %s", wantToRelease, released);
      }

      LockPlaceHolder currentLocked = current;
      if (currentLocked == wantToRelease) {
         tryAcquire(wantToRelease);
      }
   }

   /**
    * @return the current lock owner or {@code null} if it is not acquired.
    */
   public Object getLockOwner() {
      LockPlaceHolder lockPlaceHolder = current;
      return lockPlaceHolder == null ? null : lockPlaceHolder.owner;
   }

   /**
    * It checks if the lock is acquired.
    * <p/>
    * A {@code false} return value does not mean the lock is free since it may have queued lock owners.
    *
    * @return {@code true} if the lock is acquired.
    */
   public boolean isLocked() {
      return current != null;
   }

   /**
    * It forces a deadlock checking.
    */
   public void deadlockCheck(DeadlockChecker deadlockChecker) {
      if (deadlockChecker == null) {
         return; //no-op
      }
      LockPlaceHolder holder = current;
      if (holder != null) {
         for (LockPlaceHolder pending : pendingRequest) {
            pending.checkDeadlock(deadlockChecker, holder.owner);
         }
      }
   }

   /**
    * It tests if the lock has the lock owner.
    * <p/>
    * It return {@code true} if the lock owner is the current lock owner or it in the queue.
    *
    * @param lockOwner the lock owner to test.
    * @return {@code true} if it contains the lock owner.
    */
   public boolean containsLockOwner(Object lockOwner) {
      return lockOwners.containsKey(lockOwner);
   }

   private void onCanceled(LockPlaceHolder canceled) {
      if (trace) {
         log.tracef("Release lock for %s. It was canceled.", canceled.owner);
      }
      LockPlaceHolder currentLocked = current;
      if (currentLocked == canceled) {
         tryAcquire(canceled);
      }
   }

   private boolean casRelease(LockPlaceHolder lockPlaceHolder) {
      return cas(lockPlaceHolder, null);
   }

   private boolean remove(Object lockOwner) {
      return lockOwners.remove(lockOwner) != null;
   }

   private void triggerReleased() {
      if (releaseRunnable != null) {
         releaseRunnable.run();
      }
   }

   private boolean cas(LockPlaceHolder release, LockPlaceHolder acquire) {
      boolean cas = OWNER_UPDATER.compareAndSet(this, release, acquire);
      if (trace) {
         log.tracef("Lock Owner CAS(%s, %s) => %s", release, acquire, cas);
      }
      return cas;
   }

   private void tryAcquire(LockPlaceHolder release) {
      LockPlaceHolder toRelease = release;
      do {
         LockPlaceHolder toAcquire = pendingRequest.peek();
         if (trace) {
            log.tracef("Try acquire. Next in queue=%s. Current=%s", toAcquire, current);
         }
         if (toAcquire == null && toRelease == null) {
            return;
         } else if (toAcquire == null) {
            //nothing to acquire, but we have to release the current.
            if (casRelease(toRelease)) {
               toRelease = null;
               continue; //in the meanwhile, we could have a new request. recheck!
            }
            return;
         }
         if (cas(toRelease, toAcquire)) {
            //we set the current lock owner, so we must remove it from the queue
            pendingRequest.remove(toAcquire);
            if (toAcquire.setAcquire()) {
               if (trace) {
                  log.tracef("%s successfully acquired the lock.", toAcquire);
               }
               return;
            }
            if (trace) {
               log.tracef("%s failed to acquire (invalid state). Retrying.", toAcquire);
            }
            //oh oh, probably the nextPending Timed-Out. we are going to retry with the next in queue
            toRelease = toAcquire;
         } else {
            if (trace) {
               log.tracef("Unable to acquire. Lock is held.");
            }
            //other thread already set the current lock owner
            return;
         }
      } while (true);
   }

   private LockPlaceHolder createLockInfo(Object lockOwner, long time, TimeUnit timeUnit) {
      return new LockPlaceHolder(lockOwner, timeService.expectedEndTime(time, timeUnit));
   }

   private class LockPlaceHolder implements ExtendedLockPromise {

      private final Object owner;
      private final long timeout;
      private final CompletableFuture<LockState> notifier;
      @SuppressWarnings("CanBeFinal")
      volatile LockState lockState;

      private LockPlaceHolder(Object owner, long timeout) {
         this.owner = owner;
         this.timeout = timeout;
         lockState = LockState.WAITING;
         notifier = new CompletableFuture<>();
      }

      @Override
      public boolean isAvailable() {
         checkTimeout();
         return lockState != LockState.WAITING;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {
         do {
            LockState currentState = lockState;
            switch (currentState) {
               case WAITING:
                  checkTimeout();
                  await(notifier, timeService.remainingTime(timeout, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
                  break;
               case ACQUIRED:
                  return; //acquired!
               case RELEASED:
                  throw new IllegalStateException("Lock already released!");
               case TIMED_OUT:
                  cleanup();
                  throw new TimeoutException("Timeout waiting for lock.");
               case DEADLOCKED:
                  cleanup();
                  throw new DeadlockDetectedException("DeadLock detected");
               default:
                  throw new IllegalStateException("Unknown lock state: " + currentState);
            }
         } while (true);
      }

      @Override
      public void addListener(LockListener listener) {
         notifier.thenAccept(listener::onEvent);
      }

      @Override
      public InvocationStage toInvocationStage() {
         return toInvocationStage(() -> new TimeoutException("Timeout waiting for lock."));
      }

      @Override
      public void cancel(LockState state) {
         checkValidCancelState(state);
         do {
            LockState currentState = lockState;
            switch (currentState) {
               case WAITING:
                  if (casState(LockState.WAITING, state)) {
                     onCanceled(this);
                     notifyListeners();
                     return;
                  }
                  break;
               case ACQUIRED: //no-op, a thread is inside the critical section.
               case TIMED_OUT:
               case DEADLOCKED:
               case RELEASED:
                  return; //no-op, the lock is in final state.
               default:
                  throw new IllegalStateException("Unknown lock state " + currentState);

            }
         } while (true);
      }

      @Override
      public Object getRequestor() {
         return owner;
      }

      @Override
      public Object getOwner() {
         LockPlaceHolder owner = current;
         return owner != null ? owner.owner : null;
      }

      @Override
      public InvocationStage toInvocationStage(Supplier<TimeoutException> timeoutSupplier) {
         if (notifier.isDone()) {
            return checkState(notifier.getNow(lockState), SyncInvocationStage::new,
                  SimpleAsyncInvocationStage::new, timeoutSupplier);
         }
         return new SimpleAsyncInvocationStage(notifier.thenApplyAsync(state -> {
            Object rv = checkState(state, () -> null, throwable -> throwable, timeoutSupplier);
            if (rv != null) {
               throw (RuntimeException) rv;
            }
            return null;
         }, executor));
      }

      @Override
      public String toString() {
         return "LockPlaceHolder{" +
               "lockState=" + lockState +
               ", owner=" + owner +
               '}';
      }

      private <T> T checkState(LockState state, Supplier<T> acquired, Function<Throwable, T> exception, Supplier<TimeoutException> timeoutSupplier) {
         switch (state) {
            case ACQUIRED:
               return acquired.get();
            case RELEASED:
               return exception.apply(new IllegalStateException("Lock already released!"));
            case TIMED_OUT:
               cleanup();
               return exception.apply(timeoutSupplier.get());
            case DEADLOCKED:
               cleanup();
               return exception.apply(new DeadlockDetectedException("DeadLock detected"));
            default:
               return exception.apply(new IllegalStateException("Unknown lock state: " + state));
         }
      }

      private void checkValidCancelState(LockState state) {
         if (state != LockState.TIMED_OUT && state != LockState.DEADLOCKED) {
            throw new IllegalArgumentException("LockState " + state + " is not valid to cancel.");
         }
      }

      private void checkDeadlock(DeadlockChecker checker, Object currentOwner) {
         checkTimeout(); //check timeout before checking the deadlock. check deadlock are more expensive.
         if (lockState == LockState.WAITING && //we are waiting for a lock
               !owner.equals(currentOwner) && //needed? just to be safe
               checker.deadlockDetected(owner, currentOwner) && //deadlock has been detected!
               casState(LockState.WAITING, LockState.DEADLOCKED)) { //state could have been changed to available or timed_out
            onCanceled(this);
            notifyListeners();
         }
      }

      private boolean setAcquire() {
         if (casState(LockState.WAITING, LockState.ACQUIRED)) {
            notifyListeners();
         }
         return lockState == LockState.ACQUIRED;
      }

      private boolean setReleased() {
         do {
            LockState state = lockState;
            switch (state) {
               case WAITING:
                  if (casState(state, LockState.RELEASED)) {
                     cleanup();
                     notifyListeners();
                     return true;
                  }
                  break;
               case ACQUIRED:
               case TIMED_OUT:
               case DEADLOCKED:
                  if (casState(state, LockState.RELEASED)) {
                     cleanup();
                     return true;
                  }
                  break;
               case RELEASED:
                  return false;
               default:
                  throw new IllegalStateException("Unknown lock state " + state);
            }
         } while (true);
      }

      private boolean casState(LockState expect, LockState update) {
         boolean updated = STATE_UPDATER.compareAndSet(this, expect, update);
         if (updated && trace) {
            log.tracef("State changed for %s. %s => %s", this, expect, update);
         }
         return updated;
      }

      private void cleanup() {
         if (remove(owner)) {
            triggerReleased();
         }
      }

      private void checkTimeout() {
         if (lockState == LockState.WAITING &&
               timeService.isTimeExpired(timeout) &&
               casState(LockState.WAITING, LockState.TIMED_OUT)) {
            onCanceled(this);
            notifyListeners();
         }

      }

      private void notifyListeners() {
         LockState state = lockState;
         if (state != LockState.WAITING) {
            notifier.complete(state);
         }
      }
   }
}
