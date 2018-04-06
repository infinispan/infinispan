package org.infinispan.executors;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import net.jcip.annotations.GuardedBy;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.NDC;

/**
 * Executes tasks in the given executor, but never has more than {@code maxConcurrentTasks} tasks running at the same
 * time.
 *
 * <p>A task can finish running without allowing another task to run in its stead, with {@link #executeAsync(Supplier)}.
 * A new task will only start after the {@code CompletableFuture} returned by the task has completed.</p>
 *
 * <p><em>Blocking mode.</em> If the executor is a {@link WithinThreadExecutor}, tasks will run in the thread that
 * submitted them. If there are no available permits, the caller thread will block until a permit becomes available.</p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class LimitedExecutor implements Executor {
   private static final Log log = LogFactory.getLog(LimitedExecutor.class);

   private final Lock lock = new ReentrantLock();
   private final Condition taskFinishedCondition = lock.newCondition();
   private final String name;
   private final Executor executor;
   private final boolean blocking;
   private final Runner runner = new Runner();
   private volatile boolean running = true;
   @GuardedBy("lock")
   private int availablePermits;
   @GuardedBy("lock")
   private Map<Thread, Object> threads;
   @GuardedBy("lock")
   private final Deque<Runnable> queue = new ArrayDeque<>();

   public LimitedExecutor(String name, Executor executor, int maxConcurrentTasks) {
      this.name = name;
      this.executor = executor;
      this.availablePermits = maxConcurrentTasks;
      this.blocking = executor instanceof WithinThreadExecutor;

      threads = new IdentityHashMap<>(maxConcurrentTasks);
   }

   /**
    * Stops the executor and cancels any queued tasks.
    *
    * Stop and interrupt any tasks that have already been handed to the underlying executor.
    */
   public void shutdownNow() {
      log.tracef("Stopping limited executor %s", name);
      running = false;
      lock.lock();
      try {
         queue.clear();

         for (Thread t : threads.keySet()) {
            t.interrupt();
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void execute(Runnable command) {
      if (!running)
         throw new IllegalLifecycleStateException("Limited executor " + name + " is not running!");

      if (blocking) {
         CompletableFuture<Void> f1 = new CompletableFuture<>();
         executeInternal(() -> {
            f1.complete(null);
            removePermit();
         });
         try {
            CompletableFutures.await(f1);
            command.run();
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalLifecycleStateException(ie);
         } catch (Exception e) {
            log.error("Exception in task", e);
         } finally {
            addPermit();
            tryExecute();
         }
         return;
      }
      executeInternal(command);
   }

   private void executeInternal(Runnable command) {
      lock.lock();
      try {
         queue.add(command);
      } finally {
         lock.unlock();
      }
      tryExecute();
   }

   /**
    * Similar to {@link #execute(Runnable)}, but the task can continue executing asynchronously,
    * without blocking the OS thread, while still counting against this executor's limit.
    *
    * @param asyncCommand A task that returns a non-null {@link CompletionStage},
    *                     which may be already completed or may complete at some point in the future.
    */
   public void executeAsync(Supplier<CompletionStage<Void>> asyncCommand) {
      execute(() -> {
         CompletionStage<Void> future = asyncCommand.get();
         // The current permit will be released automatically
         // If the future is null, don't reserve another permit
         assert future != null;
         removePermit();
         future.whenComplete(runner);
      });
   }

   private void tryExecute() {
      boolean addRunner = false;
      lock.lock();
      try {
         if (availablePermits > 0) {
            availablePermits--;
            addRunner = true;
         }
      } finally {
         lock.unlock();
      }
      if (addRunner) {
         executor.execute(runner);
      }
   }

   private void runTasks() {
      runnerStarting();
      while (running) {
         Runnable runnable = null;
         lock.lock();
         try {
            // If the previous task was asynchronous, we can't execute a new one on the same thread
            if (availablePermits >= 0) {
               runnable = queue.poll();
            }
            if (runnable == null) {
               availablePermits++;
               break;
            }
         } finally {
            lock.unlock();
         }

         try {
            NDC.push(name);
            runnable.run();
         } catch (Throwable t) {
            log.error("Exception in task", t);
         } finally {
            NDC.pop();
         }
      }
      runnerFinished();
   }

   private void runnerStarting() {
      lock.lock();
      try {
         Thread thread = Thread.currentThread();
         threads.put(thread, thread);
      } finally {
         lock.unlock();
      }
   }

   private void runnerFinished() {
      lock.lock();
      try {
         Thread thread = Thread.currentThread();
         threads.remove(thread);
         taskFinishedCondition.signalAll();
      } finally {
         lock.unlock();
      }
   }

   private void removePermit() {
      lock.lock();
      try {
         availablePermits--;
      } finally {
         lock.unlock();
      }
   }

   private void addPermit() {
      lock.lock();
      try {
         availablePermits++;
      } finally {
         lock.unlock();
      }
   }

   private class Runner implements Runnable, BiConsumer<Void, Throwable> {
      @Override
      public void run() {
         runTasks();
      }

      @Override
      public void accept(Void aVoid, Throwable throwable) {
         addPermit();
         tryExecute();
      }
   }
}
