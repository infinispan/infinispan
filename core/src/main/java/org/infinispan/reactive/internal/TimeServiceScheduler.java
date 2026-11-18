package org.infinispan.reactive.internal;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;

/**
 * A {@link Scheduler} that runs its tasks on another scheduler and reports the time of a {@link TimeService}.
 * <p>
 * Operators that compare deadlines against {@link Scheduler#now(TimeUnit)} then measure elapsed time with the
 * {@link TimeService}, instead of the wall clock that RxJava schedulers report by default. With the default
 * {@link TimeService}, adjustments of the system clock do not shorten or extend those deadlines.
 * </p>
 */
public class TimeServiceScheduler extends Scheduler {

   private final Scheduler delegate;
   private final TimeService timeService;

   /**
    * Creates a scheduler that runs tasks on {@code delegate} and reports the time of {@code timeService}.
    *
    * @param delegate    the scheduler that runs every task scheduled through this one
    * @param timeService the source of the time reported by this scheduler and by its workers
    */
   public TimeServiceScheduler(Scheduler delegate, TimeService timeService) {
      this.delegate = delegate;
      this.timeService = timeService;
   }

   /**
    * Reports the current time of the {@link TimeService}.
    *
    * @param unit the unit of the returned time
    * @return the time of the {@link TimeService} in {@code unit}, only meaningful as a difference to another reading
    */
   @Override
   public long now(@NonNull TimeUnit unit) {
      return unit.convert(timeService.time(), TimeUnit.NANOSECONDS);
   }

   /**
    * Creates a worker that runs its tasks on a worker of the wrapped scheduler and reports the time of the
    * {@link TimeService}.
    *
    * @return a new worker, which must be disposed once no longer needed
    */
   @Override
   public @NonNull Worker createWorker() {
      return new TimeServiceWorker(delegate.createWorker(), timeService);
   }

   private static final class TimeServiceWorker extends Worker {

      private final Worker delegate;
      private final TimeService timeService;

      public TimeServiceWorker(Worker delegate, TimeService timeService) {
         this.delegate = delegate;
         this.timeService = timeService;
      }

      @Override
      public @NonNull Disposable schedule(@NonNull Runnable run, long delay, @NonNull TimeUnit unit) {
         return delegate.schedule(run, delay, unit);
      }

      /**
       * Reports the current time of the {@link TimeService}.
       *
       * @param unit the unit of the returned time
       * @return the time of the {@link TimeService} in {@code unit}, only meaningful as a difference to another
       *         reading
       */
      @Override
      public long now(@NonNull TimeUnit unit) {
         return unit.convert(timeService.time(), TimeUnit.NANOSECONDS);
      }

      @Override
      public void dispose() {
         delegate.dispose();
      }

      @Override
      public boolean isDisposed() {
         return delegate.isDisposed();
      }
   }
}
