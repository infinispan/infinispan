package org.infinispan.commons.executors;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Galder Zamarreño
 */
public class BlockingThreadPoolExecutorFactory implements ThreadPoolExecutorFactory {

   private final int maxThreads;
   private final int coreThreads;
   private final int queueLength;
   private final long keepAlive;

   public BlockingThreadPoolExecutorFactory(
         int maxThreads, int coreThreads, int queueLength, long keepAlive) {
      this.maxThreads = maxThreads;
      this.coreThreads = coreThreads;
      this.queueLength = queueLength;
      this.keepAlive = keepAlive;
   }

   public int maxThreads() {
      return maxThreads;
   }

   public int coreThreads() {
      return coreThreads;
   }

   public int queueLength() {
      return queueLength;
   }

   public long keepAlive() {
      return keepAlive;
   }

   @Override
   public ExecutorService createExecutor(ThreadFactory threadFactory) {
      BlockingQueue<Runnable> queue = queueLength == 0 ?
            new SynchronousQueue<Runnable>() :
            new LinkedBlockingQueue<Runnable>(queueLength);

      return new ThreadPoolExecutor(coreThreads, maxThreads, keepAlive,
            TimeUnit.MILLISECONDS, queue, threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy());
   }

}
