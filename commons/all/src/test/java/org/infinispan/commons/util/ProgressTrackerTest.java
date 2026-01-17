package org.infinispan.commons.util;

import static org.infinispan.testing.Eventually.eventually;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.junit.AfterClass;
import org.junit.Test;

public class ProgressTrackerTest {

   private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

   @AfterClass
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
}
