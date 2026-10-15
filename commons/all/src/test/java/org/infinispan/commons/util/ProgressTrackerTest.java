package org.infinispan.commons.util;

import static org.infinispan.testing.Eventually.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class ProgressTrackerTest {

   private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

   @AfterAll
   public static void stopExecutor() {
      executor.shutdown();
   }

   @Test
   public void testStartingAndCompletingTracker() {
      ProgressTracker tracker = new ProgressTracker("name", executor, new ControlledTimeService(), 10, TimeUnit.MINUTES);

      tracker.addTasks(5);
      assertEquals(5, tracker.pendingTasks());

      tracker.removeTasks(3);
      assertEquals(2, tracker.pendingTasks());

      tracker.finishedAllTasks();
      assertEquals(0, tracker.pendingTasks());
   }

   @Test
   public void testStartAndComplete() {
      ProgressTracker tracker = new ProgressTracker("name", executor, new ControlledTimeService(), 10, TimeUnit.MINUTES);

      tracker.addTasks(5);
      assertEquals(5, tracker.pendingTasks());

      tracker.removeTasks(5);
      assertEquals(0, tracker.pendingTasks());

      assertEquals(ProgressTracker.Progression.PROGRESSING, tracker.currentTaskStatus());

      tracker.finishedAllTasks();
      assertEquals(0, tracker.pendingTasks());
      assertEquals(ProgressTracker.Progression.DONE, tracker.currentTaskStatus());
   }

   @Test
   public void testTaskMovesToHang() {
      ProgressTracker tracker = new ProgressTracker("name", executor, new ControlledTimeService(), 500, TimeUnit.MILLISECONDS);

      tracker.addTasks(5);
      assertEquals(ProgressTracker.Progression.IDLE, tracker.currentTaskStatus());

      tracker.addTasks(-2);
      assertEquals(ProgressTracker.Progression.PROGRESSING, tracker.currentTaskStatus());

      eventually(() -> tracker.currentTaskStatus() == ProgressTracker.Progression.HANG, 2, TimeUnit.SECONDS);

      tracker.finishedAllTasks();
      assertEquals(ProgressTracker.Progression.DONE, tracker.currentTaskStatus());
   }

   @Test
   public void testTrackerIsReUtilized() {
      ProgressTracker tracker = new ProgressTracker("name", executor, new ControlledTimeService(), 10, TimeUnit.MINUTES);

      for (int i = 0; i < 3; i++) {
         tracker.addTasks(5);
         assertEquals(ProgressTracker.Progression.IDLE, tracker.currentTaskStatus());

         tracker.addTasks(-2);
         assertEquals(ProgressTracker.Progression.PROGRESSING, tracker.currentTaskStatus());

         tracker.finishedAllTasks();
         assertEquals(ProgressTracker.Progression.DONE, tracker.currentTaskStatus());
      }
   }

   @Test
   public void testFailedTaskStopsTracking() {
      ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
      // Cancelled checks leave the queue, so an empty queue means the tracker stopped its periodic check.
      scheduler.setRemoveOnCancelPolicy(true);
      try {
         ProgressTracker tracker = new ProgressTracker("name", scheduler, new ControlledTimeService(), 10, TimeUnit.MINUTES);

         tracker.addTasks(5);
         tracker.removeTasks(2);
         assertEquals(1, scheduler.getQueue().size());

         tracker.failed(new RuntimeException("Simulated failure"));
         assertEquals(ProgressTracker.Progression.FAILED, tracker.currentTaskStatus());
         assertTrue(scheduler.getQueue().isEmpty());
      } finally {
         scheduler.shutdownNow();
      }
   }

   @Test
   public void testTasksCompletingAfterFailureAreIgnored() {
      ProgressTracker tracker = new ProgressTracker("name", executor, new ControlledTimeService(), 10, TimeUnit.MINUTES);

      tracker.addTasks(5);
      tracker.removeTasks(2);
      tracker.failed(new RuntimeException("Simulated failure"));

      // Tasks still running when the failure happened can complete afterwards.
      tracker.removeTasks(1);
      assertEquals(ProgressTracker.Progression.FAILED, tracker.currentTaskStatus());
      assertEquals(0, tracker.pendingTasks());
   }

   @Test
   public void testTrackerIsReUtilizedAfterFailure() {
      ProgressTracker tracker = new ProgressTracker("name", executor, new ControlledTimeService(), 10, TimeUnit.MINUTES);

      tracker.addTasks(5);
      tracker.removeTasks(2);
      tracker.failed(new RuntimeException("Simulated failure"));
      assertEquals(ProgressTracker.Progression.FAILED, tracker.currentTaskStatus());

      tracker.addTasks(5);
      assertEquals(ProgressTracker.Progression.IDLE, tracker.currentTaskStatus());
      assertEquals(5, tracker.pendingTasks());

      tracker.removeTasks(2);
      assertEquals(ProgressTracker.Progression.PROGRESSING, tracker.currentTaskStatus());
      assertEquals(3, tracker.pendingTasks());

      tracker.finishedAllTasks();
      assertEquals(ProgressTracker.Progression.DONE, tracker.currentTaskStatus());
   }
}
