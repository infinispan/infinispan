package org.infinispan.commons.executors;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.BlockingRejectedExecutionHandler;
import org.infinispan.commons.util.concurrent.NonBlockingRejectedExecutionHandler;

/**
 * @author Galder Zamarreño
 */
public class BlockingThreadPoolExecutorFactory implements ThreadPoolExecutorFactory<ExecutorService> {
   public static final int DEFAULT_KEEP_ALIVE_MILLIS = 60000;

   private final int maxThreads;
   private final int coreThreads;
   private final int queueLength;
   private final long keepAlive;
   private final boolean nonBlocking;

   public BlockingThreadPoolExecutorFactory(int maxThreads, int coreThreads, int queueLength, long keepAlive) {
      this(maxThreads, coreThreads, queueLength, keepAlive, false);
   }

   public BlockingThreadPoolExecutorFactory(int maxThreads, int coreThreads, int queueLength, long keepAlive,
         boolean nonBlocking) {
      this.maxThreads = maxThreads;
      this.coreThreads = coreThreads;
      this.queueLength = queueLength;
      this.keepAlive = keepAlive;
      this.nonBlocking = nonBlocking;
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
   public boolean createsNonBlockingThreads() {
      return nonBlocking;
   }

   @Override
   public ExecutorService createExecutor(ThreadFactory threadFactory) {
      BlockingQueue<Runnable> queue = queueLength == 0 ? new SynchronousQueue<>() : new LinkedBlockingQueue<>(queueLength);

      if (nonBlocking) {
         if (!(threadFactory instanceof NonBlockingThreadFactory)) {
            throw new IllegalStateException("Executor factory configured to be non blocking and received a thread" +
                  " factory that can create blocking threads!");
         }
      }

      return new ThreadPoolExecutor(coreThreads, maxThreads, keepAlive,
            TimeUnit.MILLISECONDS, queue, threadFactory,
            nonBlocking ? NonBlockingRejectedExecutionHandler.getInstance() : BlockingRejectedExecutionHandler.getInstance());
   }

   @Override
   public void validate() {
      if (coreThreads < 0)
         throw CONFIG.illegalValueThreadPoolParameter("core threads", ">= 0");

      if (maxThreads <= 0)
         throw CONFIG.illegalValueThreadPoolParameter("max threads", "> 0");

      if (maxThreads < coreThreads)
         throw CONFIG.illegalValueThreadPoolParameter(
               "max threads and core threads", "max threads >= core threads");

      if (keepAlive < 0)
         throw CONFIG.illegalValueThreadPoolParameter("keep alive time", ">= 0");

      if (queueLength < 0)
         throw CONFIG.illegalValueThreadPoolParameter("work queue length", ">= 0");
   }

   @Override
   public String toString() {
      return "BlockingThreadPoolExecutorFactory{" +
            "maxThreads=" + maxThreads +
            ", coreThreads=" + coreThreads +
            ", queueLength=" + queueLength +
            ", keepAlive=" + keepAlive +
            '}';
   }

   public static BlockingThreadPoolExecutorFactory create(int maxThreads, int queueSize, boolean nonBlocking) {
      int coreThreads = queueSize == 0 ? 1 : maxThreads;
      return new BlockingThreadPoolExecutorFactory(maxThreads, coreThreads, queueSize, DEFAULT_KEEP_ALIVE_MILLIS,
            nonBlocking);
   }

}
