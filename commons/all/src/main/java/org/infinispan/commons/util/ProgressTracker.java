package org.infinispan.commons.util;

import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

@ThreadSafe
public final class ProgressTracker {

   private static final Log log = LogFactory.getLog("LIFECYCLE");

   private final String name;
   private final ScheduledExecutorService executor;
   private final TimeService timeService;
   private final long delay;
   private final TimeUnit unit;
   private final State state;

   public ProgressTracker(String name, ScheduledExecutorService executor, TimeService timeService, long delay, TimeUnit unit) {
      this.name = name;
      this.executor = executor;
      this.timeService = timeService;
      this.state = new State();
      this.delay = delay;
      this.unit = unit;
   }

   public void addTasks(long value) {
      state.addTasks(value);
   }

   public void removeTasks(long value) {
      state.addTasks(-value);
   }

   public void finishedAllTasks() {
      state.completed();
   }

   public long pendingTasks() {
      return state.pending();
   }

   public Progression currentTaskStatus() {
      return state.status();
   }

   /**
    * Holds the internal state of the task progress.
    *
    * <p>
    * This runnable tracks how many operations are pending to verify if it has stall or is progressing. These operations
    * mutate many variables simultaneously. These operations should happen atomically. To guarantee the correct update,
    * we utilize a reentrant lock to perform read and write operations.
    * </p>
    */
   @ThreadSafe
   private final class State implements Runnable {
      private final Lock lock = new ReentrantLock();
      private long pending;
      private long lastCheck;
      private Progression status = Progression.IDLE;
      private Instant startedAt = null;
      private ScheduledFuture<?> progression;

      public long pending() {
         lock.lock();
         try {
            return pending;
         } finally {
            lock.unlock();
         }
      }

      public Progression status() {
         lock.lock();
         try {
            return status;
         } finally {
            lock.unlock();
         }
      }

      public void addTasks(long value) {
         lock.lock();
         try {
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

            if (progression == null)
               progression = executor.scheduleAtFixedRate(state, delay, delay, unit);
         } finally {
            lock.unlock();
         }
      }

      public void completed() {
         lock.lock();
         try {
            status = Progression.DONE;

            if (startedAt != null)
               log.taskDone(name, startedAt, timeService.instant());

            reset();
         } finally {
            lock.unlock();
         }
      }

      @GuardedBy("lock")
      private void reset() {
         pending = 0;
         lastCheck = -1;
         startedAt = null;

         if (progression != null) {
            progression.cancel(true);
            progression = null;
         }
      }

      @Override
      public void run() {
         lock.lock();
         try {
            if (status == Progression.DONE)
               return;

            if (lastCheck == pending)
               status = Progression.HANG;

            log.taskProgression(name, pending, lastCheck, status.name());
            lastCheck = pending;
         } finally {
            lock.unlock();
         }
      }
   }

   public enum Progression {
      IDLE,
      PROGRESSING,
      HANG,
      DONE,
   }
}
