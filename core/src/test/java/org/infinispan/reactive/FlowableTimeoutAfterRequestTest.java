package org.infinispan.reactive;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.processors.PublishProcessor;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import io.reactivex.rxjava3.subscribers.TestSubscriber;

/**
 * Tests for {@link FlowableTimeoutAfterRequest}.
 *
 * <p>
 * The operator must fail the subscription when the source does not emit an item while the downstream has
 * outstanding demand, and it must stay quiet while the downstream has no demand at all. Every test drives time
 * through a {@link TestScheduler}, so none of them depends on the wall clock.
 * </p>
 *
 * @since 16.4
 */
@Test(groups = "unit", testName = "reactive.FlowableTimeoutAfterRequestTest")
public class FlowableTimeoutAfterRequestTest extends AbstractInfinispanTest {

   private static final long TIMEOUT = 100;
   private static final long AWAIT_SECONDS = 10;

   /**
    * A downstream with a bounded number of in flight operations, such as {@code flatMapCompletable}, replenishes
    * the demand from inside {@code onNext} whenever an operation completes right away. The source then stalls,
    * and the replenished demand must still time out.
    */
   public void testTimesOutWhenDemandReplenishedInsideOnNext() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = new TestSubscriber<>(1);
      withTimeout(source, clock).doOnNext(item -> subscriber.request(1)).subscribe(subscriber);

      source.onNext(1);
      clock.advanceTimeBy(TIMEOUT, MILLISECONDS);
      subscriber.assertValue(1).assertError(TimeoutException.class);
   }

   /**
    * The demand is replenished from another thread while {@code onNext} is still running, as happens when a
    * bounded consumer completes its operations asynchronously.
    */
   public void testTimesOutWhenDemandReplenishedFromAnotherThread() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = new TestSubscriber<>(1);
      withTimeout(source, clock)
            .doOnNext(item -> fork(() -> subscriber.request(1)).get(AWAIT_SECONDS, SECONDS))
            .subscribe(subscriber);

      source.onNext(1);
      clock.advanceTimeBy(TIMEOUT, MILLISECONDS);
      subscriber.assertValue(1).assertError(TimeoutException.class);
   }

   /**
    * A replenishing request that is held inside the upstream still opens a timeout window. The source never
    * emits while the request is held, so the timeout must fire one window after that request was made.
    *
    * @throws Exception if the test fails or a forked request does not finish in time
    */
   public void testTimesOutWhileReplenishingRequestIsHeldUpstream() throws Exception {
      TestScheduler clock = new TestScheduler();
      AtomicInteger requests = new AtomicInteger();
      CountDownLatch requesting = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      Flowable<Integer> source = Flowable.<Integer>never().doOnRequest(n -> {
         // Only the second request, the replenishing one, is held.
         if (requests.incrementAndGet() == 2) {
            requesting.countDown();
            assertThat(release.await(AWAIT_SECONDS, SECONDS)).isTrue();
         }
      });
      TestSubscriber<Integer> subscriber = withTimeout(source, clock).test(1);

      clock.advanceTimeBy(20, MILLISECONDS);
      Future<Void> request = fork(() -> subscriber.request(1));
      try {
         assertThat(requesting.await(AWAIT_SECONDS, SECONDS)).isTrue();
         clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
         subscriber.assertEmpty();
         clock.advanceTimeBy(1, MILLISECONDS);
         subscriber.assertError(TimeoutException.class);
      } finally {
         release.countDown();
         request.get(AWAIT_SECONDS, SECONDS);
      }
   }

   /**
    * The first request, made while subscribing, is held inside the upstream. The timeout must still fire, and
    * cancel the source, while that request is held.
    *
    * @throws Exception if the test fails or the forked subscription does not finish in time
    */
   public void testTimesOutWhileFirstRequestIsHeldUpstream() throws Exception {
      TestScheduler clock = new TestScheduler();
      CountDownLatch requesting = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicBoolean cancelled = new AtomicBoolean();
      Flowable<Integer> source = Flowable.<Integer>never()
            .doOnRequest(n -> {
               requesting.countDown();
               assertThat(release.await(AWAIT_SECONDS, SECONDS)).isTrue();
            })
            .doOnCancel(() -> cancelled.set(true));
      TestSubscriber<Integer> subscriber = new TestSubscriber<>(1);

      // The subscriber requests while subscribing, so subscribing is what gets held.
      Future<Void> subscription = fork(() -> withTimeout(source, clock).subscribe(subscriber));
      try {
         assertThat(requesting.await(AWAIT_SECONDS, SECONDS)).isTrue();
         clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
         assertThat(cancelled.get()).as("The window must not end early").isFalse();
         clock.advanceTimeBy(1, MILLISECONDS);
         assertThat(cancelled.get()).as("The timeout must fire while the first request is held").isTrue();
      } finally {
         release.countDown();
         subscription.get(AWAIT_SECONDS, SECONDS);
      }
      subscriber.assertNoValues().assertError(TimeoutException.class);
   }

   /**
    * A bounded consumer replenishes the demand on every item. Each replenishment opens a fresh window, but the
    * operator keeps watching it with the wakeup it already has instead of scheduling one per request.
    */
   public void testReplenishingDemandKeepsSingleTimerTask() {
      TestScheduler clock = new TestScheduler();
      AtomicInteger scheduled = new AtomicInteger();
      Scheduler countingScheduler = new Scheduler() {
         @Override
         public Worker createWorker() {
            Worker worker = clock.createWorker();
            return new Worker() {
               @Override
               public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                  scheduled.incrementAndGet();
                  return worker.schedule(task, delay, unit);
               }

               @Override
               public long now(TimeUnit unit) {
                  // The default reads the wall clock, the operator must see the virtual one.
                  return worker.now(unit);
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
      };
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = new TestSubscriber<>(1);
      withTimeout(source, countingScheduler).doOnNext(item -> subscriber.request(1)).subscribe(subscriber);

      for (int i = 0; i < 10; i++) {
         source.onNext(i);
      }
      assertThat(scheduled.get()).as("Wakeups scheduled for 11 requests").isEqualTo(1);

      clock.advanceTimeBy(TIMEOUT, MILLISECONDS);
      subscriber.assertValueCount(10).assertError(TimeoutException.class);
   }

   /**
    * An item arrives before the window ends, while more demand is outstanding, and the downstream takes a while to
    * handle it. The arrival shows the source is making progress, so the next window counts from the arrival and the
    * slow handling must not turn into a timeout.
    */
   public void testOnTimeItemHandledSlowlyDoesNotTimeOut() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      // Two requested, so one is still outstanding while the first item is handled.
      TestSubscriber<Integer> subscriber = new TestSubscriber<>(2);
      withTimeout(source, clock)
            .doOnNext(item -> clock.advanceTimeBy(20, MILLISECONDS))
            .subscribe(subscriber);

      clock.advanceTimeBy(TIMEOUT - 10, MILLISECONDS);
      // Arrives at 90 ms, and the downstream finishes handling it at 110 ms, past the first window.
      source.onNext(1);
      subscriber.assertValue(1).assertNoErrors();

      // The window counts from the arrival at 90 ms.
      clock.advanceTimeBy(TIMEOUT - 21, MILLISECONDS);
      subscriber.assertNoErrors();
      clock.advanceTimeBy(1, MILLISECONDS);
      subscriber.assertError(TimeoutException.class);
   }

   public void testNoTimeoutWhileDemandIsZero() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = withTimeout(source, clock).test(0);

      clock.advanceTimeBy(TIMEOUT * 2, MILLISECONDS);
      subscriber.assertEmpty();

      // The delivered item consumes all the demand, so the source stalling afterwards is not a timeout.
      subscriber.request(1);
      source.onNext(1);
      clock.advanceTimeBy(TIMEOUT * 2, MILLISECONDS);
      subscriber.assertValue(1).assertNoErrors().assertNotComplete();
      subscriber.cancel();
   }

   public void testTimesOutWhenDemandArrivesAfterIdlePeriod() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = withTimeout(source, clock).test(1);

      source.onNext(1);
      clock.advanceTimeBy(TIMEOUT * 5, MILLISECONDS);
      subscriber.assertValue(1).assertNoErrors().assertNotComplete();

      // The source is still stalled, so this demand can never be satisfied.
      subscriber.request(1);
      clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
      subscriber.assertNoErrors();
      clock.advanceTimeBy(1, MILLISECONDS);
      subscriber.assertError(TimeoutException.class);
   }

   /**
    * Demand that arrives right before the window of an earlier, idle, period ends must get a full window of its
    * own.
    */
   public void testDemandAfterIdleGetsFullWindow() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = withTimeout(source, clock).test(1);

      source.onNext(1);
      clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
      subscriber.request(1);
      clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
      subscriber.assertValue(1).assertNoErrors();
      clock.advanceTimeBy(1, MILLISECONDS);
      subscriber.assertError(TimeoutException.class);
   }

   public void testAdditionalDemandRenewsTimeout() {
      TestScheduler clock = new TestScheduler();
      TestSubscriber<Integer> subscriber = withTimeout(Flowable.never(), clock).test(1);

      clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
      subscriber.request(1);
      clock.advanceTimeBy(TIMEOUT - 1, MILLISECONDS);
      subscriber.assertEmpty();
      clock.advanceTimeBy(1, MILLISECONDS);
      subscriber.assertError(TimeoutException.class);
   }

   public void testNoTimeoutAfterSourceCompletes() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = withTimeout(source, clock).test(1);

      source.onComplete();
      clock.advanceTimeBy(TIMEOUT, MILLISECONDS);
      subscriber.assertResult();
   }

   public void testNoTimeoutAfterCancellation() {
      TestScheduler clock = new TestScheduler();
      PublishProcessor<Integer> source = PublishProcessor.create();
      TestSubscriber<Integer> subscriber = withTimeout(source, clock).test(1);

      subscriber.cancel();
      clock.advanceTimeBy(TIMEOUT, MILLISECONDS);
      subscriber.assertEmpty();
      assertThat(source.hasSubscribers()).as("Cancelling must also cancel the source").isFalse();
   }

   public void testTimeoutSwitchesToFallback() {
      TestScheduler clock = new TestScheduler();
      IllegalStateException failure = new IllegalStateException("store timeout");
      TestSubscriber<Integer> subscriber = new FlowableTimeoutAfterRequest<>(Flowable.<Integer>never(), TIMEOUT,
            MILLISECONDS, clock, Flowable.error(failure)).test(1);

      clock.advanceTimeBy(TIMEOUT, MILLISECONDS);
      subscriber.assertError(failure);
   }

   private static Flowable<Integer> withTimeout(Flowable<Integer> source, Scheduler scheduler) {
      return new FlowableTimeoutAfterRequest<>(source, TIMEOUT, MILLISECONDS, scheduler, null);
   }
}
