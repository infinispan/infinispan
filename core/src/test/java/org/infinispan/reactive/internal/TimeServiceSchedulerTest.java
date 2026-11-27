package org.infinispan.reactive.internal;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.TestScheduler;

/**
 * Tests for {@link TimeServiceScheduler}.
 *
 * <p>
 * The scheduler must report the time of its {@link org.infinispan.commons.time.TimeService}, never the clock of the
 * scheduler it runs tasks on. Every test controls the time, so none of them depends on the wall clock.
 * </p>
 *
 * @since 16.4
 */
@Test(groups = "unit", testName = "reactive.internal.TimeServiceSchedulerTest")
public class TimeServiceSchedulerTest extends AbstractInfinispanTest {

   public void testClockFollowsTimeService() {
      ControlledTimeService timeService = new ControlledTimeService("clock", 1_000);
      // The task scheduler's own clock stays at zero, so only the time service can explain the readings.
      Scheduler scheduler = new TimeServiceScheduler(new TestScheduler(), timeService);
      Scheduler.Worker worker = scheduler.createWorker();
      try {
         assertThat(scheduler.now(MILLISECONDS)).isEqualTo(1_000);
         assertThat(worker.now(MILLISECONDS)).isEqualTo(1_000);

         timeService.advance(250, MILLISECONDS);
         assertThat(scheduler.now(MILLISECONDS)).isEqualTo(1_250);
         assertThat(worker.now(NANOSECONDS)).isEqualTo(MILLISECONDS.toNanos(1_250));
      } finally {
         worker.dispose();
      }
   }

   public void testTasksRunOnWrappedScheduler() {
      ControlledTimeService timeService = new ControlledTimeService("clock", 1_000);
      TestScheduler tasks = new TestScheduler();
      Scheduler.Worker worker = new TimeServiceScheduler(tasks, timeService).createWorker();
      AtomicInteger runs = new AtomicInteger();
      try {
         worker.schedule(runs::incrementAndGet, 100, MILLISECONDS);

         timeService.advance(100, MILLISECONDS);
         assertThat(runs.get()).as("Advancing the time service must not run tasks").isZero();
         tasks.advanceTimeBy(100, MILLISECONDS);
         assertThat(runs.get()).as("Tasks run when the wrapped scheduler reaches their delay").isEqualTo(1);
      } finally {
         worker.dispose();
      }
   }

   public void testDisposingWorkerCancelsPendingTasks() {
      TestScheduler tasks = new TestScheduler();
      Scheduler.Worker worker = new TimeServiceScheduler(tasks, new ControlledTimeService()).createWorker();
      AtomicInteger runs = new AtomicInteger();

      worker.schedule(runs::incrementAndGet, 100, MILLISECONDS);
      worker.dispose();
      tasks.advanceTimeBy(100, MILLISECONDS);

      assertThat(worker.isDisposed()).isTrue();
      assertThat(runs.get()).as("A disposed worker must not run its pending tasks").isZero();
   }
}
