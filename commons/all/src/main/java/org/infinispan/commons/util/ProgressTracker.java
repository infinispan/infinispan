package org.infinispan.commons.util;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public final class ProgressTracker {

   private static final Log log = LogFactory.getLog(ProgressTracker.class);

   private final String name;
   private final ScheduledExecutorService executor;
   private final TimeService timeService;
   private final long delay;
   private final TimeUnit unit;

   @GuardedBy("state")
   private final State state;

   @GuardedBy("state")
   private ScheduledFuture<?> progression;

   public ProgressTracker(String name, ScheduledExecutorService executor, TimeService timeService, long delay, TimeUnit unit) {
      this.name = name;
      this.executor = executor;
      this.timeService = timeService;
      this.state = new State();
      this.delay = delay;
      this.unit = unit;
   }

   public void addTasks(long value) {
      synchronized (state) {
         if (progression == null) {
            progression = executor.scheduleAtFixedRate(state, delay, delay, unit);
         }

         state.addTasks(value);
      }
   }

   public void removeTasks(long value) {
      synchronized (state) {
         if (progression == null)
            throw new IllegalStateException("Task tracking not initialized");

         state.addTasks(-value);
      }
   }

   public void finishedAllTasks() {
      synchronized (state) {
         if (progression == null) return;

         progression.cancel(true);
         progression = null;
         state.completed();
      }
   }

   public long pendingTasks() {
      synchronized (state) {
         return state.pending;
      }
   }

   public Progression currentTaskStatus() {
      synchronized (state) {
         return state.status;
      }
   }

   private final class State implements Runnable {
      private long pending;
      private long lastCheck;
      private Progression status = Progression.IDLE;
      private Instant startedAt = null;

      public void reset() {
         pending = 0;
         lastCheck = -1;
         startedAt = null;
      }

      public void addTasks(long value) {
         if (value < 0) {
            if (startedAt == null)
               throw new IllegalStateException("Removing tasks from a completed tracker: " + name);

            status = Progression.PROGRESSING;
         }

         // If the tracker is initializing or restarting, we need to track the time the operations started.
         if (status == Progression.IDLE || status == Progression.DONE) {
            startedAt = timeService.instant();

            // The status goes back to idle the first time it reinitialize.
            status = Progression.IDLE;
         }

         pending += value;
      }

      public void completed() {
         reset();
         status = Progression.DONE;
         log.taskDone(name, startedAt, timeService.instant());
      }

      @Override
      public synchronized void run() {
         if (status == Progression.DONE)
            return;

         if (lastCheck == pending)
            status = Progression.HANG;

         log.taskProgression(name, pending, lastCheck, status.name());
         lastCheck = pending;
      }
   }

   public enum Progression {
      IDLE,
      PROGRESSING,
      HANG,
      DONE,
   }
}
