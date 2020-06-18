package org.infinispan.factories.threads;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.executors.NonBlockingResource;
import org.infinispan.commons.util.concurrent.BlockingRejectedExecutionHandler;
import org.jboss.threads.EnhancedQueueExecutor;
import org.jboss.threads.management.ManageableThreadPoolExecutorService;

/**
 * Executor Factory used for blocking executors which utilizes {@link EnhancedQueueExecutor} internally.
 * @author wburns
 */
public class EnhancedQueueExecutorFactory extends AbstractThreadPoolExecutorFactory<ManageableThreadPoolExecutorService> {
   protected EnhancedQueueExecutorFactory(int maxThreads, int coreThreads, int queueLength, long keepAlive) {
      super(maxThreads, coreThreads, queueLength, keepAlive);
   }

   public static EnhancedQueueExecutorFactory create(int maxThreads, int queueSize) {
      int coreThreads = queueSize == 0 ? 1 : maxThreads;
      return new EnhancedQueueExecutorFactory(maxThreads, coreThreads, queueSize,
            NonBlockingThreadPoolExecutorFactory.DEFAULT_KEEP_ALIVE_MILLIS);
   }

   @Override
   public ManageableThreadPoolExecutorService createExecutor(ThreadFactory factory) {
      if (factory instanceof NonBlockingResource) {
         throw new IllegalStateException("Executor factory configured to be blocking and received a thread" +
               " factory that creates non-blocking threads!");
      }
      EnhancedQueueExecutor.Builder builder = new EnhancedQueueExecutor.Builder();
      builder.setThreadFactory(factory);
      builder.setCorePoolSize(coreThreads);
      builder.setMaximumPoolSize(maxThreads);
      builder.setGrowthResistance(0.0f);
      builder.setMaximumQueueSize(queueLength);
      builder.setKeepAliveTime(keepAlive, TimeUnit.MILLISECONDS);

      EnhancedQueueExecutor enhancedQueueExecutor = builder.build();
      enhancedQueueExecutor.setHandoffExecutor(task ->
         BlockingRejectedExecutionHandler.getInstance().rejectedExecution(task, enhancedQueueExecutor));
      return enhancedQueueExecutor;
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
}
