package org.infinispan.commons.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public class CachedThreadPoolExecutorFactory implements ThreadPoolExecutorFactory {

   private static final CachedThreadPoolExecutorFactory INSTANCE = new CachedThreadPoolExecutorFactory();

   private CachedThreadPoolExecutorFactory() {
      // singleton
   }

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
