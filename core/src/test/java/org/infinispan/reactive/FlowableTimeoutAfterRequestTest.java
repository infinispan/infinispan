package org.infinispan.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.test.AbstractInfinispanTest;
import org.reactivestreams.Subscription;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.processors.PublishProcessor;
import io.reactivex.rxjava3.schedulers.TestScheduler;

/**
 * Tests for {@link FlowableTimeoutAfterRequest}.
 *
 * <p>
 * The operator must fail the subscription when the source does not emit an item while the downstream has
 * outstanding demand, and it must stay quiet while the downstream has no demand at all.
 * </p>
 *
 * @since 16.4
 */
@Test(groups = "unit", testName = "reactive.FlowableTimeoutAfterRequestTest")
public class FlowableTimeoutAfterRequestTest extends AbstractInfinispanTest {

   private static final long TIMEOUT_MS = 100;
   private static final long AWAIT_SECONDS = 10;

   /**
    * A downstream with a bounded number of in flight operations, such as {@code flatMapCompletable}, replenishes
    * the demand from inside {@code onNext} whenever an operation completes right away. The timer armed by that
    * request must survive the delivery it was issued from, otherwise the subscription never times out.
    */
   public void testTimesOutWhenDemandReplenishedInsideOnNext() throws InterruptedException {
      TestScheduler scheduler = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, subscription -> subscription.request(1));
      timeoutFlowable(source, scheduler).subscribe(subscriber);

      // The source emits a single item and then stalls while the subscriber still has demand.
      source.onNext(1);

      scheduler.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue(subscriber.awaitTermination(0, TimeUnit.SECONDS),
            "The subscription must be terminated by the timeout");
      assertInstanceOf(TimeoutException.class, subscriber.error());
      assertEquals(1, subscriber.received());
   }

   /**
    * The same scenario, with the demand replenished by another thread while {@code onNext} is still running.
    */
   public void testTimesOutWhenDemandReplenishedFromAnotherThread() throws InterruptedException {
      TestScheduler scheduler = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, subscription -> {
         // Wait for the request to complete before returning, so the delivery always observes the demand
         // as already replenished. This is the interleaving that loses the timer.
         Future<Void> requested = fork(() -> {
            subscription.request(1);
         });
         requested.get(AWAIT_SECONDS, TimeUnit.SECONDS);
      });
      timeoutFlowable(source, scheduler).subscribe(subscriber);

      source.onNext(1);

      scheduler.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue(subscriber.awaitTermination(0, TimeUnit.SECONDS),
            "The subscription must be terminated by the timeout");
      assertInstanceOf(TimeoutException.class, subscriber.error());
   }

   /**
    * A subscriber that stops requesting is not late, it is simply not asking for anything. The operator must not
    * fail it, no matter how long it stays idle.
    */
   public void testNoTimeoutWhileDemandIsZero() throws InterruptedException {
      TestScheduler scheduler = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, null);
      timeoutFlowable(source, scheduler).subscribe(subscriber);

      source.onNext(1);

      scheduler.advanceTimeBy(TIMEOUT_MS * 10, TimeUnit.MILLISECONDS);
      assertFalse(subscriber.awaitTermination(0, TimeUnit.MILLISECONDS),
            "The subscription must not be terminated while it has no outstanding demand");
      assertNull(subscriber.error());

      subscriber.cancel();
   }

   /**
    * Demand that arrives long after the previous item must arm the timer again.
    */
   public void testTimesOutWhenDemandArrivesLate() throws InterruptedException {
      TestScheduler scheduler = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, null);
      timeoutFlowable(source, scheduler).subscribe(subscriber);

      source.onNext(1);

      scheduler.advanceTimeBy(TIMEOUT_MS * 5, TimeUnit.MILLISECONDS);
      assertFalse(subscriber.awaitTermination(0, TimeUnit.MILLISECONDS),
            "The subscription must not be terminated while it has no outstanding demand");

      // The source is still stalled, so this demand can never be satisfied.
      subscriber.request(1);

      scheduler.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue(subscriber.awaitTermination(0, TimeUnit.SECONDS),
            "The subscription must be terminated by the timeout");
      assertInstanceOf(TimeoutException.class, subscriber.error());
   }

   /**
    * {@code scheduleTimeout()} runs concurrently: {@code onNext} arms the timer on the upstream thread while
    * {@code request()} arms it on whichever thread replenishes the demand. Each call first takes a new timer index
    * and then installs its task, and the two steps are not atomic, so the installs can land in the opposite order
    * to the indices. The task left installed is then the one holding the stale index, which {@code onTimeout}
    * discards, and the subscription is left with demand and no timer.
    *
    * <p>
    * The scheduler below blocks the call made from {@code onNext} between taking its index and installing its task,
    * so the later {@code request()} installs first. This is the ordering the operator has to survive.
    * </p>
    */
   public void testTimerSurvivesReorderedConcurrentScheduling() throws Exception {
      TestScheduler scheduler = new TestScheduler();
      CountDownLatch parked = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      // Call 1 arms the timer for the initial request(1), call 2 is the one made from onNext.
      AtomicInteger scheduleCalls = new AtomicInteger();
      ReorderingScheduler reordering = new ReorderingScheduler(scheduler, scheduleCalls, 2, parked, release);

      PublishProcessor<Integer> source = PublishProcessor.create();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, null);
      new FlowableTimeoutAfterRequest<>(source.hide(), TIMEOUT_MS, TimeUnit.MILLISECONDS, reordering, null)
            .subscribe(subscriber);

      // Emit on a separate thread: it parks inside the operator while arming the timer from onNext.
      Future<Void> emission = fork(() -> {
         source.onNext(1);
      });

      assertTrue(parked.await(AWAIT_SECONDS, TimeUnit.SECONDS), "onNext should be arming the timer");

      // Replenish the demand while the emitting thread is parked. request() returns only once it has installed
      // its own timer, so releasing afterwards guarantees the stale install happens last.
      subscriber.request(1);
      release.countDown();
      emission.get(AWAIT_SECONDS, TimeUnit.SECONDS);

      // The source is stalled and the subscriber has outstanding demand, so the subscription must time out.
      scheduler.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue(subscriber.awaitTermination(0, TimeUnit.SECONDS),
            "The subscription must be terminated by the timeout");
      assertInstanceOf(TimeoutException.class, subscriber.error());
   }

   /**
    * Installing a timer has to reject a task whose generation is older than the installed one, as a single atomic
    * step. Checking first and installing afterwards is not enough: two callers can both pass the check and then
    * install in the opposite order, so the older task ends up installed, {@code onTimeout} discards it as stale,
    * and the subscription keeps its demand with no timer.
    *
    * <p>
    * This drives the ordering through the operator itself. The arming done by {@code onNext} is parked while it
    * holds the older generation, the {@code request()} that follows takes a newer generation and installs, and
    * only then is the older install attempted. The older task must be thrown away, the newer one must stay armed,
    * and the subscription must still be failed by it.
    * </p>
    */
   public void testStaleTimerInstallDoesNotReplaceNewerTimer() throws Exception {
      TestScheduler scheduler = new TestScheduler();
      CountDownLatch parked = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicInteger scheduleCalls = new AtomicInteger();
      // Call 1 arms for the initial request(1); call 2 is made from onNext and is the one parked.
      ReorderingScheduler reordering = new ReorderingScheduler(scheduler, scheduleCalls, 2, parked, release);

      PublishProcessor<Integer> source = PublishProcessor.create();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, null);
      new FlowableTimeoutAfterRequest<>(source.hide(), TIMEOUT_MS, TimeUnit.MILLISECONDS, reordering, null)
            .subscribe(subscriber);

      Future<Void> emission = fork(() -> {
         source.onNext(1);
      });
      assertTrue(parked.await(AWAIT_SECONDS, TimeUnit.SECONDS), "onNext should be arming the timer");

      // The newer generation is taken and installed while the older one is still parked.
      subscriber.request(1);
      TrackedDisposable newer = reordering.lastTask();
      assertNotNull(newer, "request() should have scheduled a task");
      assertFalse(newer.discardedByOperator(), "the newest task must stay armed");

      // Let the older install attempt land last. It must be discarded rather than replace the newer task.
      release.countDown();
      emission.get(AWAIT_SECONDS, TimeUnit.SECONDS);

      // These look at whether the operator itself threw a task away, not at whether the task has since been
      // cancelled by the worker, so they stay valid however long the timeout takes to fire.
      TrackedDisposable stale = reordering.taskOfCall(2);
      assertNotNull(stale, "onNext should have scheduled a task");
      eventually("the stale task must be discarded", stale::discardedByOperator);
      assertFalse(newer.discardedByOperator(), "the newer task must not be replaced by the stale one");

      // What ultimately matters: the stalled source with outstanding demand is still failed by the timeout.
      scheduler.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertTrue(subscriber.awaitTermination(0, TimeUnit.SECONDS),
            "The subscription must be terminated by the timeout");
      assertInstanceOf(TimeoutException.class, subscriber.error());
   }

   public void testIdleTimerCannotExpireNewDemandDuringUpstreamRequest() {
      TestScheduler scheduler = new TestScheduler();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, null);
      var parent = new FlowableTimeoutAfterRequest.TimeoutSubscriber<Integer>(subscriber, TIMEOUT_MS,
            TimeUnit.MILLISECONDS, scheduler.createWorker(), null);
      AtomicInteger requests = new AtomicInteger();
      parent.onSubscribe(new Subscription() {
         @Override
         public void request(long n) {
            if (requests.incrementAndGet() == 2) {
               // Expire the previous idle timer before the new upstream request returns.
               scheduler.advanceTimeBy(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
         }

         @Override
         public void cancel() { }
      });
      parent.onNext(1);
      subscriber.request(1);
      assertNull(subscriber.error(), "An idle timer must not expire newly published demand");
      scheduler.advanceTimeBy(TIMEOUT_MS - 1, TimeUnit.MILLISECONDS);
      assertNull(subscriber.error());
      scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
      assertInstanceOf(TimeoutException.class, subscriber.error());
   }

   public void testCompetingInstallsRetryGenerationCheck() throws Exception {
      TestScheduler scheduler = new TestScheduler();
      RecordingSubscriber subscriber = new RecordingSubscriber(1, null);
      var parent = new FlowableTimeoutAfterRequest.TimeoutSubscriber<Integer>(subscriber, TIMEOUT_MS,
            TimeUnit.MILLISECONDS, scheduler.createWorker(), null);
      CountDownLatch checked = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      FlowableTimeoutAfterRequest.Timer initial = mock(FlowableTimeoutAfterRequest.Timer.class);
      Disposable initialTask = Disposable.empty();
      when(initial.task()).thenReturn(initialTask);
      when(initial.generation()).thenAnswer(invocation -> {
         if (Thread.currentThread().getName().equals("older-timer-install")) {
            checked.countDown();
            assertTrue(release.await(AWAIT_SECONDS, TimeUnit.SECONDS));
         }
         return 0L;
      });
      parent.timerTask.set(initial);
      Disposable older = Disposable.empty();
      Disposable newer = Disposable.empty();
      Future<Void> install = fork(() -> {
         Thread thread = Thread.currentThread();
         String name = thread.getName();
         try {
            thread.setName("older-timer-install");
            parent.installTimer(1, older);
         } finally {
            thread.setName(name);
         }
      });
      try {
         assertTrue(checked.await(AWAIT_SECONDS, TimeUnit.SECONDS));
         // The older installer has read the old slot. Replace it before its CAS, forcing a retry.
         parent.installTimer(2, newer);
      } finally {
         release.countDown();
      }
      install.get(AWAIT_SECONDS, TimeUnit.SECONDS);
      assertEquals(2, parent.timerTask.get().generation());
      assertTrue(older.isDisposed());
      assertFalse(newer.isDisposed());
      parent.cancel();
   }

   private Flowable<Integer> timeoutFlowable(PublishProcessor<Integer> source, Scheduler scheduler) {
      return new FlowableTimeoutAfterRequest<>(source.hide(), TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduler, null);
   }

   /**
    * Delegating scheduler that parks the n-th scheduling call after the operator has taken its timer index but
    * before it installs the task, so a concurrent call can install its own task first.
    */
   private static final class ReorderingScheduler extends Scheduler {
      private final Scheduler delegate;
      private final AtomicInteger calls;
      private final int parkOnCall;
      private final CountDownLatch parked;
      private final CountDownLatch release;
      private final Map<Integer, TrackedDisposable> tasks = new ConcurrentHashMap<>();
      private final AtomicReference<TrackedDisposable> lastTask = new AtomicReference<>();

      private ReorderingScheduler(Scheduler delegate, AtomicInteger calls, int parkOnCall,
            CountDownLatch parked, CountDownLatch release) {
         this.delegate = delegate;
         this.calls = calls;
         this.parkOnCall = parkOnCall;
         this.parked = parked;
         this.release = release;
      }

      TrackedDisposable taskOfCall(int call) {
         return tasks.get(call);
      }

      TrackedDisposable lastTask() {
         return lastTask.get();
      }

      @Override
      public Worker createWorker() {
         Worker worker = delegate.createWorker();
         return new Worker() {
            @Override
            public Disposable schedule(Runnable run, long delay, TimeUnit unit) {
               int call = calls.incrementAndGet();
               if (call == parkOnCall) {
                  parked.countDown();
                  try {
                     release.await(AWAIT_SECONDS, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                  }
               }
               TrackedDisposable tracked = new TrackedDisposable(worker.schedule(run, delay, unit));
               tasks.put(call, tracked);
               lastTask.set(tracked);
               return tracked;
            }

            @Override
            public void dispose() {
               worker.dispose();
            }

            @Override
            public boolean isDisposed() {
               return worker.isDisposed();
            }
         };
      }
   }

   /**
    * Records whether the operator threw a scheduled task away, as opposed to the task simply being cancelled
    * later when the worker is disposed. Only the former goes through this wrapper.
    */
   private static final class TrackedDisposable implements Disposable {
      private final Disposable delegate;
      private volatile boolean discarded;

      private TrackedDisposable(Disposable delegate) {
         this.delegate = delegate;
      }

      /** True once the operator decided not to keep this task. */
      boolean discardedByOperator() {
         return discarded;
      }

      @Override
      public void dispose() {
         discarded = true;
         delegate.dispose();
      }

      @Override
      public boolean isDisposed() {
         return discarded || delegate.isDisposed();
      }
   }

   @FunctionalInterface
   private interface OnNextAction {
      void accept(Subscription subscription) throws Exception;
   }

   private static final class RecordingSubscriber implements FlowableSubscriber<Integer> {
      private final CountDownLatch terminated = new CountDownLatch(1);
      private final AtomicReference<Throwable> error = new AtomicReference<>();
      private final AtomicInteger received = new AtomicInteger();
      private final long initialRequest;
      private final OnNextAction onNextAction;
      private volatile Subscription subscription;

      private RecordingSubscriber(long initialRequest, OnNextAction onNextAction) {
         this.initialRequest = initialRequest;
         this.onNextAction = onNextAction;
      }

      @Override
      public void onSubscribe(Subscription s) {
         subscription = s;
         s.request(initialRequest);
      }

      @Override
      public void onNext(Integer item) {
         received.incrementAndGet();
         if (onNextAction == null) {
            return;
         }
         try {
            onNextAction.accept(subscription);
         } catch (Exception e) {
            error.compareAndSet(null, e);
            terminated.countDown();
         }
      }

      @Override
      public void onError(Throwable t) {
         error.compareAndSet(null, t);
         terminated.countDown();
      }

      @Override
      public void onComplete() {
         terminated.countDown();
      }

      boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         return terminated.await(timeout, unit);
      }

      void request(long n) {
         subscription.request(n);
      }

      void cancel() {
         subscription.cancel();
      }

      Throwable error() {
         return error.get();
      }

      int received() {
         return received.get();
      }
   }
}
