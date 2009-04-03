package org.horizon.profiling.testinternals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Essentially a delegate to an ExecutorService, but a special one that is only used by perf tests so it can be ignored
 * when profiling.
 */
public class TaskRunner {
   ExecutorService exec;

   public TaskRunner(int numThreads) {
      this.exec = Executors.newFixedThreadPool(numThreads);
   }

   public void execute(Runnable r) {
      exec.execute(r);
   }

   public void stop() throws InterruptedException {
      exec.shutdown();
      while (!exec.awaitTermination(30, TimeUnit.SECONDS)) Thread.sleep(30);
   }
}
