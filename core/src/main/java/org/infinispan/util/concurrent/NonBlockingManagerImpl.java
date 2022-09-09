package org.infinispan.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

@Scope(Scopes.GLOBAL)
public class NonBlockingManagerImpl implements NonBlockingManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   @Inject ScheduledExecutorService scheduler;
   @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   @Inject Executor executor;

   @Override
   public AutoCloseable scheduleWithFixedDelay(Supplier<CompletionStage<?>> supplier, long initialDelay, long delay,
         TimeUnit unit, Predicate<? super Throwable> mayRepeatOnThrowable) {
      ReschedulingTask task = new ReschedulingTask(supplier, delay, unit, mayRepeatOnThrowable);
      synchronized (task) {
         task.future = scheduler.schedule(task, initialDelay, unit);
      }
      return task;
   }

   @Override
   public <T> void complete(CompletableFuture<? super T> future, T value) {
      // This is just best effort to eliminate context switch if there are no dependents.
      if (future.getNumberOfDependents() > 0) {
         executor.execute(() -> future.complete(value));
      } else {
         future.complete(value);
      }
   }

   @Override
   public void completeExceptionally(CompletableFuture<?> future, Throwable t) {
      // This is just best effort to eliminate context switch if there are no dependents.
      if (future.getNumberOfDependents() > 0) {
         executor.execute(() -> future.completeExceptionally(t));
      } else {
         future.completeExceptionally(t);
      }
   }

   private class ReschedulingTask implements AutoCloseable, Runnable {
      @GuardedBy("this")
      private Future<?> future;

      private final Supplier<CompletionStage<?>> supplier;
      private final long delay;
      private final TimeUnit unit;
      private final Predicate<? super Throwable> mayRepeatOnThrowable;

      private ReschedulingTask(Supplier<CompletionStage<?>> supplier, long delay, TimeUnit unit,
            Predicate<? super Throwable> mayRepeatOnThrowable) {
         this.supplier = supplier;
         this.delay = delay;
         this.unit = unit;
         this.mayRepeatOnThrowable = mayRepeatOnThrowable;
      }

      @Override
      public void run() {
         CompletionStage<?> stage = supplier.get();
         stage.whenComplete((v, t) -> {
            if (t != null) {
               if (mayRepeatOnThrowable == null || !mayRepeatOnThrowable.test(t)) {
                  log.scheduledTaskEncounteredThrowable(supplier, t);
                  return;
               }
               log.tracef(t, "There was an error in submitted periodic non blocking task with supplier %s, configured to resubmit", supplier);
            }
            boolean isRunning;
            synchronized (this) {
               isRunning = future != null;
            }
            if (isRunning) {
               Future<?> newFuture = scheduler.schedule(this, delay, unit);
               boolean shouldCancel = false;
               synchronized (this) {
                  if (future == null) {
                     shouldCancel = true;
                  } else {
                     future = newFuture;
                  }
               }
               if (shouldCancel) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Periodic non blocking task with supplier %s was cancelled while rescheduling.", supplier);
                  }
                  newFuture.cancel(true);
               }
            } else if (log.isTraceEnabled()) {
               log.tracef("Periodic non blocking task with supplier %s was cancelled prior to execution.", supplier);
            }

         });
      }

      @Override
      public void close() throws Exception {
         if (log.isTraceEnabled()) {
            log.tracef("Periodic non blocking task with supplier %s was cancelled.", supplier);
         }
         Future<?> cancelFuture;
         synchronized (this) {
            cancelFuture = future;
            future = null;
         }
         if (cancelFuture != null) {
            cancelFuture.cancel(false);
         }
      }
   }
}
