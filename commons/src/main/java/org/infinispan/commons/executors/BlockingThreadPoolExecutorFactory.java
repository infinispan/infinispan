package org.infinispan.commons.executors;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

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

   private static final Log log = LogFactory.getLog(BlockingThreadPoolExecutorFactory.class);

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

   @Override
   public void validate() {
      if (coreThreads < 0)
         throw log.illegalValueThreadPoolParameter("core threads", ">= 0");

      if (maxThreads <= 0)
         throw log.illegalValueThreadPoolParameter("max threads", "> 0");

      if (maxThreads < coreThreads)
         throw log.illegalValueThreadPoolParameter(
               "max threads and core threads", "max threads >= core threads");

      if (keepAlive < 0)
         throw log.illegalValueThreadPoolParameter("keep alive time", ">= 0");

      if (queueLength < 0)
         throw log.illegalValueThreadPoolParameter("work queue length", ">= 0");
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

   public static BlockingThreadPoolExecutorFactory create(int maxThreads, int queueSize) {
      int coreThreads = queueSize == 0 ? 1 : maxThreads;
      return new BlockingThreadPoolExecutorFactory(
            maxThreads, coreThreads, queueSize, 60000);
   }

}
