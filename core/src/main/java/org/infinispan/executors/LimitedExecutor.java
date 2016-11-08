package org.infinispan.executors;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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
   private final Condition condition = lock.newCondition();
   private final String name;
   private final Executor executor;
   private final boolean blocking;
   private int availablePermits;
   private final Deque<Runnable> queue = new ArrayDeque<>();
   private final Runner runner = new Runner();

   public LimitedExecutor(String name, Executor executor, int maxConcurrentTasks) {
      this.name = name;
      this.executor = executor;
      this.availablePermits = maxConcurrentTasks;
      this.blocking = executor instanceof WithinThreadExecutor;
   }

   /**
    * When stopping, cancel any queued tasks.
    */
   public void cancelQueuedTasks() {
      lock.lock();
      try {
         queue.clear();
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void execute(Runnable command) {
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
            log.debug("Exception in blocking task", e);
         } finally {
            addPermit();
            tryExecute();
         }
         return;
      }
      executeInternal(command);
   }

   public void executeInternal(Runnable command) {
      lock.lock();
      try {
         queue.add(command);
      } finally {
         lock.unlock();
      }
      tryExecute();
   }

   public void executeAsync(Supplier<CompletableFuture<Void>> command) {
      execute(() -> {
         CompletableFuture<Void> future = command.get();
         if (!future.isDone()) {
            removePermit();
            future.whenComplete(runner);
         }
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
      while (true) {
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
