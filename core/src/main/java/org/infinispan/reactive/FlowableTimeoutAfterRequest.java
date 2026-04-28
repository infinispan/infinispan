package org.infinispan.reactive;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.disposables.SequentialDisposable;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import io.reactivex.rxjava3.internal.util.BackpressureHelper;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

/**
 * A timeout operator that only arms the timer after the downstream has called {@code request(n)}.
 * <p>
 * Unlike the standard RxJava {@code timeout()} operator which starts timing from subscription,
 * this operator defers the timer until demand exists. The timer resets on each emitted item and
 * stops when outstanding demand drops to zero. This prevents spurious timeouts when subscribers
 * use backpressure and delay their requests.
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

   static final class TimeoutSubscriber<T> extends AtomicLong
         implements FlowableSubscriber<T>, Subscription {

      static final long TERMINATED_INDEX = Long.MIN_VALUE;

      final Subscriber<? super T> downstream;
      final long timeout;
      final TimeUnit unit;
      final Scheduler.Worker worker;
      final Publisher<? extends T> onTimeout;

      final AtomicLong requested = new AtomicLong();
      final AtomicReference<Subscription> upstream = new AtomicReference<>();
      final SequentialDisposable timerTask = new SequentialDisposable();

      TimeoutSubscriber(Subscriber<? super T> downstream, long timeout, TimeUnit unit,
            Scheduler.Worker worker, Publisher<? extends T> onTimeout) {
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
         if (SubscriptionHelper.validate(n)) {
            long total = BackpressureHelper.add(requested, n);
            if (log.isTraceEnabled()) {
               log.tracef("TimeoutAfterRequest[%08x] request(%d), outstanding demand: %d, arming timer",
                     System.identityHashCode(this), n, total + n);
            }
            upstream.get().request(n);
            scheduleTimeout();
         }
      }

      @Override
      public void cancel() {
         set(TERMINATED_INDEX);
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] cancelled", System.identityHashCode(this));
         }
         SubscriptionHelper.cancel(upstream);
         timerTask.dispose();
         worker.dispose();
      }

      @Override
      public void onNext(T t) {
         if (get() == TERMINATED_INDEX) {
            return;
         }

         long remaining = BackpressureHelper.produced(requested, 1);
         downstream.onNext(t);

         if (remaining > 0) {
            if (log.isTraceEnabled()) {
               log.tracef("TimeoutAfterRequest[%08x] received item, remaining demand: %d, resetting timer",
                     System.identityHashCode(this), remaining);
            }
            scheduleTimeout();
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("TimeoutAfterRequest[%08x] received item, no remaining demand, stopping timer",
                     System.identityHashCode(this));
            }
            timerTask.update(null);
         }
      }

      @Override
      public void onError(Throwable t) {
         if (terminate() == TERMINATED_INDEX) {
            RxJavaPlugins.onError(t);
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] upstream error: %s", System.identityHashCode(this), t);
         }
         timerTask.dispose();
         worker.dispose();
         downstream.onError(t);
      }

      @Override
      public void onComplete() {
         if (terminate() == TERMINATED_INDEX) {
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] upstream completed", System.identityHashCode(this));
         }
         timerTask.dispose();
         worker.dispose();
         downstream.onComplete();
      }

      long terminate() {
         long idx;
         do {
            idx = get();
            if (idx == TERMINATED_INDEX) {
               return TERMINATED_INDEX;
            }
         } while (!compareAndSet(idx, TERMINATED_INDEX));
         return idx;
      }

      void scheduleTimeout() {
         long current, next;
         do {
            current = get();
            if (current == TERMINATED_INDEX) {
               return;
            }
            next = current + 1;
         } while (!compareAndSet(current, next));
         long finalNext = next;
         Disposable d = worker.schedule(() -> onTimeout(finalNext), timeout, unit);
         timerTask.update(d);
      }

      void onTimeout(long expectedIdx) {
         if (requested.get() <= 0) {
            if (log.isTraceEnabled()) {
               log.tracef("TimeoutAfterRequest[%08x] timer fired but no outstanding demand, ignoring",
                     System.identityHashCode(this));
            }
            return;
         }
         if (!compareAndSet(expectedIdx, TERMINATED_INDEX)) {
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("TimeoutAfterRequest[%08x] timed out after %d %s with %d outstanding demand",
                  System.identityHashCode(this), timeout, unit, requested.get());
         }
         SubscriptionHelper.cancel(upstream);
         worker.dispose();

         if (onTimeout != null) {
            onTimeout.subscribe(new OnTimeoutSubscriber<>(downstream));
         } else {
            downstream.onError(new java.util.concurrent.TimeoutException(
                  "TimeoutAfterRequest[" + String.format("%08x", System.identityHashCode(this))
                        + "] timed out after " + timeout + " " + unit + " with "
                        + requested.get() + " outstanding demand"));
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
