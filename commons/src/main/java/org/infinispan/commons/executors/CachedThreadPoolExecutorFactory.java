package org.infinispan.commons.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public enum CachedThreadPoolExecutorFactory implements ThreadPoolExecutorFactory<ExecutorService> {

   INSTANCE;

   @Override
   public ExecutorService createExecutor(ThreadFactory factory) {
      return Executors.newCachedThreadPool(factory);
   }

   @Override
   public void validate() {
      // No-op
   }

   public static CachedThreadPoolExecutorFactory create() {
      return INSTANCE;
   }
}
