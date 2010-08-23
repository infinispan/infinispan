package org.infinispan.profiling.testinternals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Essentially a delegate to an ExecutorService, but a special one that is only used by perf tests so it can be ignored
 * when profiling.
 */
public class TaskRunner {
   ExecutorService exec;

   public TaskRunner(int numThreads) {
      this(numThreads, false);
   }
   public TaskRunner(int numThreads, final boolean warmup) {
      final AtomicInteger counter = new AtomicInteger(0);
      final ThreadGroup tg = new ThreadGroup(Thread.currentThread().getThreadGroup(), warmup ? "WarmupLoadGenerators" : "LoadGenerators");
      this.exec = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

         public Thread newThread(Runnable r) {
            return new Thread(tg, r, (warmup ? "WarmupLoadGenerator-" : "LoadGenerator-") + counter.incrementAndGet());
         }
      });
   }

   public void execute(Runnable r) {
      exec.execute(r);
   }

   public void stop() throws InterruptedException {
      exec.shutdown();
      while (!exec.awaitTermination(30, TimeUnit.SECONDS)) Thread.sleep(30);
   }
}
