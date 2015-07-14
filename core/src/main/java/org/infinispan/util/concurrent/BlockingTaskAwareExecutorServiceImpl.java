package org.infinispan.util.concurrent;

import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A special executor service that accepts a {@code BlockingRunnable}. This special runnable gives hints about the code
 * to be running in order to avoiding put a runnable that will block the thread. In this way, only when the runnable
 * says that is ready, it is sent to the real executor service
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class BlockingTaskAwareExecutorServiceImpl extends AbstractExecutorService implements BlockingTaskAwareExecutorService {

   private static final Log log = LogFactory.getLog(BlockingTaskAwareExecutorServiceImpl.class);
   private final Queue<BlockingRunnable> blockedTasks;
   private final ExecutorService executorService;
   private final TimeService timeService;
   private final ControllerThread controllerThread;
   private volatile boolean shutdown;

   public BlockingTaskAwareExecutorServiceImpl(String controllerThreadName, ExecutorService executorService, TimeService timeService) {
      this.blockedTasks = new ConcurrentLinkedQueue<>();
      this.executorService = executorService;
      this.timeService = timeService;
      this.shutdown = false;
      this.controllerThread = new ControllerThread(controllerThreadName);
      controllerThread.start();
   }

   @Override
   public final void execute(BlockingRunnable runnable) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      if (runnable.isReady()) {
         doExecute(runnable);
         if (log.isTraceEnabled()) {
            log.tracef("Added a new task directly: %s task(s) are waiting", blockedTasks.size());
         }
      } else {
         //we no longer submit directly to the executor service.
         blockedTasks.offer(runnable);
         controllerThread.checkForReadyTask();
         if (log.isTraceEnabled()) {
            log.tracef("Added a new task to the queue: %s task(s) are waiting", blockedTasks.size());
         }
      }
   }

   @Override
   public void shutdown() {
      shutdown = true;
   }

   @Override
   public List<Runnable> shutdownNow() {
      shutdown = true;
      controllerThread.interrupt();
      List<Runnable> runnableList = new LinkedList<>();
      runnableList.addAll(executorService.shutdownNow());
      runnableList.addAll(blockedTasks);
      return runnableList;
   }

   @Override
   public boolean isShutdown() {
      return shutdown;
   }

   @Override
   public boolean isTerminated() {
      return shutdown && blockedTasks.isEmpty() && executorService.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      final long endTime = timeService.expectedEndTime(timeout, unit);
      long waitTime = timeService.remainingTime(endTime, TimeUnit.MILLISECONDS);
      while (!blockedTasks.isEmpty() && waitTime > 0) {
         Thread.sleep(waitTime);
         waitTime = timeService.remainingTime(endTime, TimeUnit.MILLISECONDS);
      }
      return isTerminated();
   }

   @Override
   public final void checkForReadyTasks() {
      controllerThread.checkForReadyTask();
   }

   @Override
   public void execute(Runnable command) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      if (command instanceof BlockingRunnable) {
         execute((BlockingRunnable) command);
      } else {
         execute(new RunnableWrapper(command));
      }
   }

   private void doExecute(BlockingRunnable runnable) {
      try {
         executorService.execute(runnable);
      } catch (RejectedExecutionException rejected) {
         //put it back!
         blockedTasks.offer(runnable);
      }
   }

   private class ControllerThread extends Thread {

      private final Semaphore semaphore;
      private volatile boolean interrupted;

      public ControllerThread(String controllerThreadName) {
         super(controllerThreadName);
         semaphore = new Semaphore(0);
      }

      public void checkForReadyTask() {
         semaphore.release();
      }

      @Override
      public void interrupt() {
         interrupted = true;
         super.interrupt();
         semaphore.release();

      }

      @Override
      public void run() {
         while (!interrupted) {
            try {
               semaphore.acquire();
            } catch (InterruptedException e) {
               return;
            }
            semaphore.drainPermits();
            int size = blockedTasks.size();
            if (size == 0) {
               continue;
            }
            ArrayDeque<BlockingRunnable> readyList = new ArrayDeque<>(size);
            for (Iterator<BlockingRunnable> iterator = blockedTasks.iterator(); iterator.hasNext(); ) {
               BlockingRunnable runnable = iterator.next();
               if (runnable.isReady()) {
                  iterator.remove();
                  readyList.addLast(runnable);
               }
            }

            if (log.isTraceEnabled()) {
               log.tracef("Tasks to be executed=%s, still pending=~%s", readyList.size(), size);
            }

            BlockingRunnable runnable;
            while ((runnable = readyList.pollFirst()) != null) {
               doExecute(runnable);
            }
         }
      }
   }

   private static class RunnableWrapper implements BlockingRunnable {

      private final Runnable runnable;

      private RunnableWrapper(Runnable runnable) {
         this.runnable = runnable;
      }

      @Override
      public boolean isReady() {
         return true;
      }

      @Override
      public void run() {
         runnable.run();
      }
   }
}
