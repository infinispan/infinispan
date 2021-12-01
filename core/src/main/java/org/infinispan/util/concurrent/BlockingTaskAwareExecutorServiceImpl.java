package org.infinispan.util.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A special executor service that accepts a {@code BlockingRunnable}. This special runnable gives hints about the code
 * to be running in order to avoiding put a runnable that will block the thread. In this way, only when the runnable
 * says that is ready, it is sent to the real executor service
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Scope(Scopes.GLOBAL)
public class BlockingTaskAwareExecutorServiceImpl extends AbstractExecutorService implements BlockingTaskAwareExecutorService {

   private static final Log log = LogFactory.getLog(BlockingTaskAwareExecutorServiceImpl.class);
   private final Queue<BlockingRunnable> blockedTasks;
   private final ExecutorService executorService;
   private final TimeService timeService;
   private volatile boolean shutdown;

   private final AtomicInteger requestCounter = new AtomicInteger();

   public BlockingTaskAwareExecutorServiceImpl(ExecutorService executorService, TimeService timeService) {
      this.blockedTasks = new ConcurrentLinkedQueue<>();
      this.executorService = executorService;
      this.timeService = timeService;
      this.shutdown = false;
   }

   @Stop
   void stop() {
      // This method only runs in the server, in embedded mode
      // BlockingTaskAwareExecutorServiceImpl is wrapped in a LazyInitializingScheduledExecutorService
      // In the server, we do not need to stop the executorService, which has its own lifecycle
      // But we need to stop retrying tasks
      shutdown = true;
   }

   @Override
   public final void execute(BlockingRunnable runnable) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      if (runnable.isReady()) {
         doExecute(runnable);
         if (log.isTraceEnabled()) {
            log.tracef("Added a new task directly: %d task(s) are waiting", blockedTasks.size());
         }
      } else {
         //we no longer submit directly to the executor service.
         blockedTasks.offer(runnable);
         checkForReadyTasks();
         if (log.isTraceEnabled()) {
            log.tracef("Added a new task to the queue: %d task(s) are waiting", blockedTasks.size());
         }
      }
   }

   @Override
   public void shutdown() {
      shutdown = true;
      executorService.shutdown();
   }

   @Override
   public List<Runnable> shutdownNow() {
      shutdown = true;
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
      if (!blockedTasks.isEmpty()) {
         tryBlockedTasks();
      }
   }

   @Override
   public void execute(Runnable command) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      if (command instanceof BlockingRunnable) {
         execute((BlockingRunnable) command);
      } else {
         try {
            executorService.execute(command);
         } catch (RejectedExecutionException rejected) {
            //put it back!
            blockedTasks.offer(new RunnableWrapper(command));
            checkForReadyTasks();
         }
      }
   }

   public ExecutorService getExecutorService() {
      return executorService;
   }

   /**
    * Attempts to run any blocked tasks that are now able to be ran. Note that if concurrent threads invoke this
    * method only one can run the given tasks. If an additional thread attempts to run the given tasks it will
    * restart the original one.
    */
   private void tryBlockedTasks() {
      int counter = requestCounter.getAndIncrement();
      if (counter == 0) {
         do {
            int taskExecutionCount = 0;
            int remaining = 0;
            for (Iterator<BlockingRunnable> iterator = blockedTasks.iterator(); iterator.hasNext(); ) {
               BlockingRunnable runnable = iterator.next();
               boolean ready;
               try {
                  ready = runnable.isReady();
               } catch (Exception e) {
                  log.debugf(e, "Failed to check ready state of %s, dropping.", runnable);
                  iterator.remove();
                  continue;
               }
               boolean executed = false;
               if (ready) {
                  iterator.remove();
                  executed = doExecute(runnable);
               }
               if (executed) {
                  taskExecutionCount++;
               } else {
                  remaining++;
               }
            }
            if (log.isTraceEnabled()) {
               log.tracef("Tasks executed=%s, still pending=~%s", taskExecutionCount, remaining);
            }
         } while ((counter = requestCounter.addAndGet(-counter)) != 0);
      }
   }

   private boolean doExecute(BlockingRunnable runnable) {
      try {
         executorService.execute(runnable);
         return true;
      } catch (RejectedExecutionException rejected) {
         if (!shutdown) {
            //put it back!
            blockedTasks.offer(runnable);
            requestCounter.incrementAndGet();
         }
         return false;
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

      @Override
      public String toString() {
         return "RunnableWrapper(" + runnable + ")";
      }
   }
}
