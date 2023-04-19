package org.infinispan.util.concurrent.locks.impl;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.infinispan.commons.util.concurrent.CompletableFutures.await;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.interceptors.ExceptionSyncInvocationStage;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockReleasedException;
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
   private static final AtomicReferenceFieldUpdater<InfinispanLock, LockRequest> OWNER_UPDATER =
         newUpdater(InfinispanLock.class, LockRequest.class, "current");
   private static final AtomicReferenceFieldUpdater<LockPlaceHolder, LockState> STATE_UPDATER =
         newUpdater(LockPlaceHolder.class, LockState.class, "lockState");


   private volatile Queue<LockRequest> pendingRequest;
   private final ConcurrentMap<Object, LockRequest> lockOwners;
   private final Runnable releaseRunnable;
   private final Executor nonBlockingExecutor;
   private TimeService timeService;
   @SuppressWarnings("CanBeFinal")
   private volatile LockRequest current;

   /**
    * Creates a new instance.
    *
    * @param nonBlockingExecutor executor that is resumed upon after a lock has been acquired or times out if waiting
    * @param timeService         the {@link TimeService} to check for timeouts.
    */
   public InfinispanLock(Executor nonBlockingExecutor, TimeService timeService) {
      this(nonBlockingExecutor, timeService, null);
   }

   /**
    * Creates a new instance.
    *
    * @param nonBlockingExecutor executor that is resumed upon after a lock has been acquired or times out if waiting
    * @param timeService         the {@link TimeService} to check for timeouts.
    * @param releaseRunnable     a {@link Runnable} that is invoked every time this lock is released.
    */
   public InfinispanLock(Executor nonBlockingExecutor, TimeService timeService, Runnable releaseRunnable) {
      this.nonBlockingExecutor = nonBlockingExecutor;
      this.timeService = timeService;
      lockOwners = new ConcurrentHashMap<>();
      current = null;
      this.releaseRunnable = releaseRunnable;
   }

   /**
    * Creates a new instance which is acquired by {@code owner}.
    * <p>
    * The {@code lockPromise} stores the reference to the {@link ExtendedLockPromise}.
    * The method {@link #acquire(Object, long, TimeUnit)} is no longer necessary to be invoked by this lock {@code owner}.
    *
    * @param nonBlockingExecutor executor that is resumed upon after a lock has been acquired or times out if waiting
    * @param timeService         the {@link TimeService} to check for timeouts.
    * @param releaseRunnable     a {@link Runnable} that is invoked every time this lock is released.
    * @param owner               the lock owner.
    * @param lockPromise         the {@link ByRef} to store the {@link ExtendedLockPromise}.
    */
   public InfinispanLock(Executor nonBlockingExecutor, TimeService timeService, Runnable releaseRunnable, Object owner, ByRef<ExtendedLockPromise> lockPromise) {
      this.nonBlockingExecutor = nonBlockingExecutor;
      this.timeService = timeService;
      lockOwners = new ConcurrentHashMap<>();
      this.releaseRunnable = releaseRunnable;
      LockAcquired promise = new LockAcquired(owner);
      current = promise;
      lockOwners.put(owner, promise);
      lockPromise.set(promise);
      if (log.isTraceEnabled()) {
         log.tracef("%s successfully acquired the lock.", lockPromise);
      }
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

      if (log.isTraceEnabled()) {
         log.tracef("Acquire lock for %s. Timeout=%s (%s)", lockOwner, time, timeUnit);
      }

      LockRequest lockPlaceHolder = lockOwners.get(lockOwner);
      if (lockPlaceHolder != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Lock owner already exists: %s", lockPlaceHolder);
         }
         return lockPlaceHolder;
      }

      lockPlaceHolder = createLockInfo(lockOwner, time, timeUnit);
      LockRequest other = lockOwners.putIfAbsent(lockOwner, lockPlaceHolder);

      if (other != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Lock owner already exists: %s", other);
         }
         return other;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Created a new one: %s", lockPlaceHolder);
      }

      addToPendingRequests(lockPlaceHolder);
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

      if (log.isTraceEnabled()) {
         log.tracef("Release lock for %s.", lockOwner);
      }

      LockRequest wantToRelease = lockOwners.get(lockOwner);
      if (wantToRelease == null) {
         if (log.isTraceEnabled()) {
            log.tracef("%s not found!", lockOwner);
         }
         //nothing to release
         return;
      }

      final boolean released = wantToRelease.setReleased();
      if (log.isTraceEnabled()) {
         log.tracef("Release lock for %s? %s", wantToRelease, released);
      }

      LockRequest currentLocked = current;
      if (currentLocked == wantToRelease) {
         tryAcquire(wantToRelease);
      }
   }

   /**
    * @return the current lock owner or {@code null} if it is not acquired.
    */
   public Object getLockOwner() {
      LockRequest lockPlaceHolder = current;
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
      LockRequest holder = current;
      if (holder != null) {
         forEachPendingRequest(request -> request.checkDeadlock(deadlockChecker, holder));
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

   private void onCanceled(LockRequest canceled) {
      if (log.isTraceEnabled()) {
         log.tracef("Release lock for %s. It was canceled.", canceled.getRequestor());
      }
      LockRequest currentLocked = current;
      if (currentLocked == canceled) {
         tryAcquire(canceled);
      }
   }

   private boolean casRelease(LockRequest lockPlaceHolder) {
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

   private boolean cas(LockRequest release, LockRequest acquire) {
      boolean cas = OWNER_UPDATER.compareAndSet(this, release, acquire);
      if (log.isTraceEnabled()) {
         log.tracef("Lock Owner CAS(%s, %s) => %s", release, acquire, cas);
      }
      return cas;
   }

   private void tryAcquire(LockRequest release) {
      LockRequest toRelease = release;
      do {
         LockRequest toAcquire = peekNextPendingRequest();
         if (log.isTraceEnabled()) {
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
            removeFromPendingRequest(toAcquire);
            if (toAcquire.setAcquire()) {
               if (log.isTraceEnabled()) {
                  log.tracef("%s successfully acquired the lock.", toAcquire);
               }
               return;
            }
            if (log.isTraceEnabled()) {
               log.tracef("%s failed to acquire (invalid state). Retrying.", toAcquire);
            }
            //oh oh, probably the nextPending Timed-Out. we are going to retry with the next in queue
            toRelease = toAcquire;
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Unable to acquire. Lock is held.");
            }
            //other thread already set the current lock owner
            return;
         }
      } while (true);
   }

   private LockRequest createLockInfo(Object lockOwner, long time, TimeUnit timeUnit) {
      return new LockPlaceHolder(lockOwner, timeService.expectedEndTime(time, timeUnit));
   }

   private void addToPendingRequests(LockRequest request) {
      if (pendingRequest == null) {
         synchronized (this) {
            if (pendingRequest == null) {
               pendingRequest = new ConcurrentLinkedQueue<>();
            }
         }
      }
      pendingRequest.add(request);
   }

   private LockRequest peekNextPendingRequest() {
      if (pendingRequest == null) {
         return null;
      }
      return pendingRequest.peek();
   }

   private void removeFromPendingRequest(LockRequest request) {
      assert pendingRequest != null;
      pendingRequest.remove(request);
   }

   private void forEachPendingRequest(Consumer<LockRequest> consumer) {
      if (pendingRequest == null) {
         return;
      }
      pendingRequest.forEach(consumer);
   }

   private static void checkValidCancelState(LockState state) {
      if (state != LockState.TIMED_OUT && state != LockState.DEADLOCKED) {
         throw new IllegalArgumentException("LockState " + state + " is not valid to cancel.");
      }
   }

   private abstract class LockRequest implements ExtendedLockPromise {

      final Object owner;

      LockRequest(Object owner) {
         this.owner = owner;
      }

      abstract boolean setAcquire();

      abstract void checkDeadlock(DeadlockChecker deadlockChecker, LockRequest holder);

      abstract boolean setReleased();

      @Override
      public final Object getRequestor() {
         return owner;
      }

      @Override
      public final Object getOwner() {
         return getLockOwner();
      }
   }

   private class LockPlaceHolder extends LockRequest {

      private final long timeout;
      private final CompletableFuture<LockState> notifier;
      @SuppressWarnings("CanBeFinal")
      volatile LockState lockState;

      private LockPlaceHolder(Object owner, long timeout) {
         super(owner);
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
                  throw new LockReleasedException("Requestor '" + owner + "' failed to acquire lock. Lock already released!");
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
         if (notifier.isDone() && !notifier.isCompletedExceptionally()) {
            listener.onEvent(notifier.join());
         } else {
            notifier.thenAccept(listener::onEvent);
         }
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
      public InvocationStage toInvocationStage(Supplier<TimeoutException> timeoutSupplier) {
         if (notifier.isDone()) {
            return checkState(notifier.getNow(lockState), InvocationStage::completedNullStage, ExceptionSyncInvocationStage::new, timeoutSupplier);
         }
         return new SimpleAsyncInvocationStage(notifier.thenApplyAsync(state -> {
            Object rv = checkState(state, () -> null, throwable -> throwable, timeoutSupplier);
            if (rv != null) {
               throw (RuntimeException) rv;
            }
            return null;
         }, nonBlockingExecutor));
      }

      @Override
      public String toString() {
         return "LockPlaceHolder{" + "lockState=" + lockState + ", owner=" + owner + '}';
      }

      @Override
      public void checkDeadlock(DeadlockChecker checker, LockRequest holder) {
         checkTimeout(); //check timeout before checking the deadlock. check deadlock are more expensive.
         Object currentOwner = holder.owner;
         if (lockState == LockState.WAITING && //we are waiting for a lock
               !owner.equals(currentOwner) && //needed? just to be safe
               checker.deadlockDetected(owner, currentOwner) && //deadlock has been detected!
               casState(LockState.WAITING, LockState.DEADLOCKED)) { //state could have been changed to available or timed_out
            onCanceled(this);
            notifyListeners();
         }
      }

      @Override
      public boolean setAcquire() {
         if (casState(LockState.WAITING, LockState.ACQUIRED)) {
            notifyListeners();
         }
         return lockState == LockState.ACQUIRED;
      }

      @Override
      public boolean setReleased() {
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

      private <T> T checkState(LockState state, Supplier<T> acquired, Function<Throwable, T> exception, Supplier<TimeoutException> timeoutSupplier) {
         switch (state) {
            case ACQUIRED:
               return acquired.get();
            case RELEASED:
               return exception.apply(new LockReleasedException("Requestor '" + owner + "' failed to acquire lock. Lock already released!"));
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

      private boolean casState(LockState expect, LockState update) {
         boolean updated = STATE_UPDATER.compareAndSet(this, expect, update);
         if (updated && log.isTraceEnabled()) {
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
         if (lockState == LockState.WAITING && timeService.isTimeExpired(timeout) && casState(LockState.WAITING, LockState.TIMED_OUT)) {
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

   private class LockAcquired extends LockRequest {

      private volatile boolean released;

      LockAcquired(Object owner) {
         super(owner);
      }

      @Override
      public void cancel(LockState cause) {
         checkValidCancelState(cause);
         //no-op, already acquired
      }

      @Override
      public InvocationStage toInvocationStage(Supplier<TimeoutException> timeoutSupplier) {
         return toInvocationStage();
      }

      @Override
      public boolean isAvailable() {
         return true;
      }

      @Override
      public void lock() {
         //no-op acquired!
      }

      @Override
      public void addListener(LockListener listener) {
         listener.onEvent(released ? LockState.RELEASED : LockState.ACQUIRED);
      }

      @Override
      public InvocationStage toInvocationStage() {
         return InvocationStage.completedNullStage();
      }

      @Override
      public boolean setAcquire() {
         throw new IllegalStateException("setAcquire() should never be invoked");
      }

      @Override
      public void checkDeadlock(DeadlockChecker deadlockChecker, LockRequest holder) {
         throw new IllegalStateException("checkDeadlock() should never be invoked");
      }

      @Override
      public boolean setReleased() {
         released = true;
         if (remove(owner)) {
            if (log.isTraceEnabled()) {
               log.tracef("State changed for %s. ACQUIRED => RELEASED", this);
            }
            triggerReleased();
            return true;
         }
         return false;
      }

      @Override
      public String toString() {
         return "LockAcquired{" + "released?=" + released + ", owner=" + owner + '}';
      }
   }
}
