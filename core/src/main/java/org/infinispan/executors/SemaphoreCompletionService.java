package org.infinispan.executors;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Executes tasks in the given executor, but never has more than {@code maxConcurrentTasks} tasks running at the same time.
 *
 * @author Dan Berindei
 * @since 7.2
 */
public class SemaphoreCompletionService<T> implements CompletionService<T> {
   private static final Log log = LogFactory.getLog(SemaphoreCompletionService.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Executor executor;
   private final CustomSemaphore semaphore;
   private final BlockingQueue<QueueingTask> queue = new LinkedBlockingQueue<>();
   private final BlockingQueue<QueueingTask> completionQueue = new LinkedBlockingQueue<>();

   public SemaphoreCompletionService(Executor executor, int maxConcurrentTasks) {
      this.executor = executor;
      this.semaphore = new CustomSemaphore(maxConcurrentTasks);
   }

   public List<? extends Future<T>> drainCompletionQueue() {
      List<QueueingTask> list = new ArrayList<QueueingTask>();
      completionQueue.drainTo(list);
      return list;
   }

   /**
    * When stopping, cancel any queued tasks.
    */
   public void cancelQueuedTasks() {
      ArrayList<QueueingTask> queuedTasks = new ArrayList<QueueingTask>();
      queue.drainTo(queuedTasks);
      for (QueueingTask task : queuedTasks) {
         task.cancel(false);
      }
   }

   /**
    * Called from a task to remove the permit that would otherwise be freed when the task finishes running
    *
    * When the asynchronous part of the task finishes, it must call {@link #backgroundTaskFinished(Callable)}
    * to make the permit available again.
    */
   public void continueTaskInBackground() {
      if (trace) log.tracef("Moving task to background, available permits %d", semaphore.availablePermits());
      //
      semaphore.removePermit();
   }

   /**
    * Signal that a task that called {@link #continueTaskInBackground()} has finished and
    * optionally execute another task on the just-freed thread.
    */
   public Future<T> backgroundTaskFinished(final Callable<T> cleanupTask) {
      QueueingTask futureTask = null;
      if (cleanupTask != null) {
         if (trace) log.tracef("Background task finished, executing cleanup task");
         futureTask = new QueueingTask(cleanupTask);
         executor.execute(futureTask);
      } else {
         semaphore.release();
         if (trace) log.tracef("Background task finished, available permits %d", semaphore.availablePermits());
         executeFront();
      }
      return futureTask;
   }

   @Override
   public Future<T> submit(final Callable<T> task) {
      QueueingTask futureTask = new QueueingTask(task);
      queue.add(futureTask);
      if (trace) log.tracef("New task submitted, tasks in queue %d, available permits %d", queue.size(), semaphore.availablePermits());
      executeFront();
      return futureTask;
   }

   @Override
   public Future<T> submit(final Runnable task, T result) {
      QueueingTask futureTask = new QueueingTask(task, result);
      queue.add(futureTask);
      if (trace) log.tracef("New task submitted, tasks in queue %d, available permits %d", queue.size(), semaphore.availablePermits());
      executeFront();
      return futureTask;
   }

   private void executeFront() {
      while (!queue.isEmpty() && semaphore.tryAcquire()) {
         QueueingTask next = queue.poll();
         if (next != null) {
            // Execute the task, and it will release the permit when it finishes
            executor.execute(next);
            // Only execute one task, if there are other tasks and permits available, they will be scheduled
            // to be executed either by the threads that released the permits or by the threads that added
            // the tasks.
            return;
         } else {
            // Perform another iteration, in case someone adds a task and skips executing it just before
            // we release the permit
            semaphore.release();
         }
      }
   }

   @Override
   public Future<T> take() throws InterruptedException {
      return completionQueue.take();
   }

   @Override
   public Future<T> poll() {
      return completionQueue.poll();
   }

   @Override
   public Future<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
      return completionQueue.poll(timeout, unit);
   }

   private class QueueingTask extends FutureTask<T> {

      public QueueingTask(Callable<T> task) {
         super(task);
      }

      public QueueingTask(Runnable runnable, Object result) {
         super(runnable, (T) result);
      }

      @Override
      public void run() {
         try {
            QueueingTask next = this;
            do {
               next.runInternal();

               // Don't run another task if the current task called startBackgroundTask()
               // and there are no more permits available
               if (semaphore.availablePermits() < 0)
                  break;
               next = queue.poll();
            } while (next != null);
         } finally {
            semaphore.release();
            if (trace) log.tracef("All queued tasks finished, available permits %d", semaphore.availablePermits());

            // In case we just got a new task between queue.poll() and semaphore.release()
            if (!queue.isEmpty()) {
               executeFront();
            }
         }
      }

      private void runInternal() {
         try {
            if (trace) log.tracef("Task started, tasks in queue %d, available permits %d", queue.size(), semaphore.availablePermits());
            super.run();
         } finally {
            completionQueue.offer(this);
            if (trace) log.tracef("Task finished, tasks in queue %d, available permits %d", queue.size(), semaphore.availablePermits());
         }
      }
   }

   /**
    * Extend {@code Semaphore} to expose the {@code reducePermits(int)} method.
    */
   private static class CustomSemaphore extends Semaphore {
      public CustomSemaphore(int permits) {
         super(permits);
      }

      protected void removePermit() {
         super.reducePermits(1);
      }
   }
}
