package org.infinispan.factories.threads;

import java.util.concurrent.ExecutorService;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;

/**
 * Static factory class for producing executor factories in the core module
 * @author wburns
 */
public class CoreExecutorFactory {
   private CoreExecutorFactory() { }

   public static ThreadPoolExecutorFactory<? extends ExecutorService> executorFactory(int maxThreads, int coreThreads,
         int queueLength, long keepAlive, boolean nonBlocking) {
      if (nonBlocking) {
         return new NonBlockingThreadPoolExecutorFactory(maxThreads, coreThreads, queueLength, keepAlive);
      }
      return new EnhancedQueueExecutorFactory(maxThreads, coreThreads, queueLength, keepAlive);
   }

   public static ThreadPoolExecutorFactory<? extends ExecutorService> executorFactory(int maxThreads, int queueLength,
         boolean nonBlocking) {
      if (nonBlocking) {
         return NonBlockingThreadPoolExecutorFactory.create(maxThreads, queueLength);
      }
      return EnhancedQueueExecutorFactory.create(maxThreads, queueLength);
   }
}
