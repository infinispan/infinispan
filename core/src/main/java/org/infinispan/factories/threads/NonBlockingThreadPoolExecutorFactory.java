package org.infinispan.factories.threads;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.executors.NonBlockingResource;
import org.infinispan.commons.util.concurrent.NonBlockingRejectedExecutionHandler;

/**
 * Executor Factory used for non blocking executors which utilizes {@link ThreadPoolExecutor} internally.
 * @author wburns
 */
public class NonBlockingThreadPoolExecutorFactory extends AbstractThreadPoolExecutorFactory<ExecutorService> {
   public static final int DEFAULT_KEEP_ALIVE_MILLIS = 60000;

   protected NonBlockingThreadPoolExecutorFactory(int maxThreads, int coreThreads, int queueLength, long keepAlive) {
      super(maxThreads, coreThreads, queueLength, keepAlive);
   }

   @Override
   public boolean createsNonBlockingThreads() {
      return true;
   }

   @Override
   public ExecutorService createExecutor(ThreadFactory threadFactory) {
      BlockingQueue<Runnable> queue = queueLength == 0 ? new SynchronousQueue<>() : new LinkedBlockingQueue<>(queueLength);

      if (!(threadFactory instanceof NonBlockingResource)) {
         throw new IllegalStateException("Executor factory configured to be non blocking and received a thread" +
               " factory that can create blocking threads!");
      }

      return new ThreadPoolExecutor(coreThreads, maxThreads, keepAlive,
            TimeUnit.MILLISECONDS, queue, threadFactory,
            NonBlockingRejectedExecutionHandler.getInstance());
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

   public static NonBlockingThreadPoolExecutorFactory create(int maxThreads, int queueSize) {
      int coreThreads = queueSize == 0 ? 1 : maxThreads;
      return new NonBlockingThreadPoolExecutorFactory(maxThreads, coreThreads, queueSize, DEFAULT_KEEP_ALIVE_MILLIS);
   }

}
