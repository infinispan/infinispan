package org.infinispan.reactive;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.reactive.internal.subscriptions.SubscriptionHelper;
import org.infinispan.reactive.internal.util.BackpressureHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

/**
 * A timeout operator that only arms the timer after the downstream has called {@code request(n)}.
 * <p>
 * Unlike the standard RxJava {@code timeout()} operator which starts timing from subscription,
 * this operator defers the timer until demand exists. Each request and each delivered item opens a
 * fresh timeout window, and the timeout only fires while the downstream has outstanding demand. This
 * prevents spurious timeouts when subscribers use backpressure and delay their requests.
 * </p>
 *
 * @param <T> the element type
 */
public final class FlowableTimeoutAfterRequest<T> extends Flowable<T> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   final Flowable<T> source;
   final long timeout;
   final TimeUnit unit;
   final Scheduler scheduler;
   final Publisher<? extends T> onTimeout;

   public FlowableTimeoutAfterRequest(Flowable<T> source, long timeout, TimeUnit unit,
         Scheduler scheduler, Publisher<? extends T> onTimeout) {
      this.source = source;
      this.timeout = timeout;
      this.unit = unit;
      this.scheduler = scheduler;
      this.onTimeout = onTimeout;
   }

   @Override
   protected void subscribeActual(Subscriber<? super T> s) {
      Scheduler.Worker worker = scheduler.createWorker();
      TimeoutSubscriber<T> parent = new TimeoutSubscriber<>(s, timeout, unit, worker, onTimeout);
      source.subscribe(parent);
   }

   /**
    * Subscribes to the source on behalf of a single downstream subscriber, and fails the subscription when the
    * source does not deliver an item in time.
    *
    * <p>
    * A timeout window opens whenever the downstream requests items, and whenever an item arrives while more
    * demand is outstanding. When a window ends while the downstream still has outstanding demand, the source is
    * Requests may arrive from any thread, including from inside {@code onNext}. Completion, failure and the timeout
    * are mutually exclusive, downstream receives at most one of them.
    * </p>
    *
    * @param <T> the element type
    */
   private static final class TimeoutSubscriber<T> extends AtomicInteger
         implements FlowableSubscriber<T>, Subscription, Runnable {

      private interface State {
         // No wakeup is scheduled, because there is no outstanding demand to watch.
         int IDLE = 0;

         // Exactly one wakeup is scheduled.
         // It watches the window until it goes idle or times out.
         int PENDING = 1;

         // The subscription completed, failed, was cancelled, or timed out.
         int TERMINATED = 2;
      }

      final Subscriber<? super T> downstream;
      final long timeout;
      final TimeUnit unit;
      final Scheduler.Worker worker;
      final Publisher<? extends T> onTimeout;

      final AtomicLong requested = new AtomicLong();
      final AtomicReference<Subscription> upstream = new AtomicReference<>();

      // When the current window ends, in nanoseconds of the scheduler's clock.
      // A request writes it before publishing the demand it covers.
      volatile long deadline;

      TimeoutSubscriber(Subscriber<? super T> downstream, long timeout, TimeUnit unit,
            Scheduler.Worker worker, Publisher<? extends T> onTimeout) {
         super(State.IDLE);
         this.downstream = downstream;
         this.timeout = timeout;
         this.unit = unit;
         this.worker = worker;
         this.onTimeout = onTimeout;
      }

      @Override
      public void onSubscribe(Subscription s) {
         if (SubscriptionHelper.setOnce(upstream, s)) {
            if (log.isTraceEnabled()) {
               log.tracef("TimeoutAfterRequest[%08x] subscribed, timeout=%d %s",
                     System.identityHashCode(this), timeout, unit);
            }
            downstream.onSubscribe(this);
         }
      }

      @Override
      public void request(long n) {
         if (!SubscriptionHelper.validate(n) || get() == State.TERMINATED)
            return;

         long timeoutNs = unit.toNanos(timeout);

         // The deadline is written before the demand is added, and a wakeup reads the demand before the deadline.
         // A wakeup that sees this demand therefor also sees this deadline.
         // The window of an earlier idle period can never expire demand that was just requested.
         deadline = now() + timeoutNs;
         long total = BackpressureHelper.add(requested, n);
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] request(%d), outstanding demand: %d (estimated)",
                  System.identityHashCode(this), n, total);
         }

         // Only one wakeup is ever scheduled.
         // A pending one picks up the new deadline when it runs.
         if (compareAndSet(State.IDLE, State.PENDING))
            scheduleWakeup(timeoutNs);

         // The window opens before the request is forwarded.
         // The upstream may hold this call, and the timeout must still fire while it is being held.
         upstream.get().request(n);
      }

      @Override
      public void cancel() {
         set(State.TERMINATED);
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] cancelled", System.identityHashCode(this));
         }
         SubscriptionHelper.cancel(upstream);
         worker.dispose();
      }

      @Override
      public void onNext(T t) {
         if (get() == State.TERMINATED)
            return;

         long remaining = BackpressureHelper.produced(requested, 1);
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] received item, remaining demand: %d",
                  System.identityHashCode(this), remaining);
         }

         // The item shows the source is making progress, so the window restarts on arrival, before sending downstream.
         // The idea is that a slow downstream should not be mistaken by a slow store.
         // A request made while the downstream handles the item opens its own window.
         if (remaining > 0)
            deadline = now() + unit.toNanos(timeout);

         // The `onNext` operation is deliberately not serialised with the timeout handling on `run()`.
         // A wakeup on the timer thread can still signal the downstream while it handles the current item.
         // Holding the signal back needs synchronising state between the two threads, so we are intentionally skipping that.
         downstream.onNext(t);
      }

      @Override
      public void onError(Throwable t) {
         if (getAndSet(State.TERMINATED) == State.TERMINATED) {
            RxJavaPlugins.onError(t);
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] upstream error: %s", System.identityHashCode(this), t);
         }

         worker.dispose();
         downstream.onError(t);
      }

      @Override
      public void onComplete() {
         if (getAndSet(State.TERMINATED) == State.TERMINATED) {
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] upstream completed", System.identityHashCode(this));
         }

         worker.dispose();
         downstream.onComplete();
      }

      /**
       * Checks whether the current timeout window has ended.
       * <p>
       * When the window has ended with demand outstanding, the subscription terminates: the source is cancelled and
       * the downstream switches to the fallback publisher, or fails with a
       * {@link java.util.concurrent.TimeoutException} when there is none. While no demand is outstanding, the window
       * is not checked again until the next request. Otherwise the window is checked again when it ends. Does
       * nothing once the subscription has terminated.
       * </p>
       */
      @Override
      public void run() {
         for (;;) {
            if (requested.get() == 0) {
               // Go idle, then look again.
               // A request racing with this wakeup either saw PENDING and relies on this wakeup.
               // Or, it saw IDLE and scheduled its own wakeup.
               if (!compareAndSet(State.PENDING, State.IDLE))
                  return;

               if (requested.get() == 0 || !compareAndSet(State.IDLE, State.PENDING))
                  return;

               continue;
            }

            // Demand first, deadline second, see the `request(n)` method.
            // Both are read separately from the CAS below, on purpose.
            // An item or a request can land in between, but only after this window has already ended with outstanding demand.
            // The timeout is due and the late event loses the race.
            // Reading and terminating in one atomic step would need synchronisation of demand, deadline, and state.
            // We are intentionally skipping the extra synchronisation.
            long remaining = deadline - now();
            if (remaining > 0) {
               scheduleWakeup(remaining);
               return;
            }

            if (compareAndSet(State.PENDING, State.TERMINATED)) {
               // Read again for the message only.
               // A late item delivered since the check above may have lowered it, but that's fine.
               timeout(requested.get());
               return;
            }

            // Already terminated by completion, failure, or cancellation.
            return;
         }
      }

      /**
       * Schedules the single wakeup that checks the timeout window.
       *
       * @param delayNanos how long from now the wakeup runs, in nanoseconds
       */
      private void scheduleWakeup(long delayNanos) {
         // The scheduled runnable is discarded once the worker is disposed.
         worker.schedule(this, delayNanos, TimeUnit.NANOSECONDS);
      }

      /**
       * Reads the scheduler's clock.
       *
       * @return the current time of the scheduler in nanoseconds, only meaningful as a difference to another
       *         reading
       */
      private long now() {
         // The clock comes from the scheduler on purpose, and it is only as good as the scheduler's clock.
         // The default schedulers from RxJava use wall clock time and could have some drift or jumps.
         // Places using this flowable should always use a monotonic clock, such as TimeServiceScheduler.
         return worker.now(TimeUnit.NANOSECONDS);
      }

      /**
       * Terminates the subscription after a timeout window ended with demand outstanding. Cancels the source and
       * switches the downstream to the fallback publisher, or fails it with a
       * {@link java.util.concurrent.TimeoutException} when there is none.
       *
       * @param outstanding the demand outstanding when the window ended, reported in the failure
       */
      private void timeout(long outstanding) {
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] timed out after %d %s with %d outstanding demand",
                  System.identityHashCode(this), timeout, unit, outstanding);
         }

         SubscriptionHelper.cancel(upstream);
         worker.dispose();

         if (onTimeout != null) {
            onTimeout.subscribe(new OnTimeoutSubscriber<T>(downstream));
         } else {
            downstream.onError(new TimeoutException(
                  "TimeoutAfterRequest[" + String.format("%08x", System.identityHashCode(this))
                        + "] timed out after " + timeout + " " + unit + " with "
                        + outstanding + " outstanding demand"));
         }
      }
   }

   record OnTimeoutSubscriber<T>(Subscriber<? super T> downstream) implements FlowableSubscriber<T> {

      @Override
      public void onSubscribe(Subscription s) {
         s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(T t) {
         downstream.onNext(t);
      }

      @Override
      public void onError(Throwable t) {
         downstream.onError(t);
      }

      @Override
      public void onComplete() {
         downstream.onComplete();
      }
   }
}
