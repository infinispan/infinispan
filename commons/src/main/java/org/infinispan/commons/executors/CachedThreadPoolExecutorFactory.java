package org.infinispan.commons.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public class CachedThreadPoolExecutorFactory implements ThreadPoolExecutorFactory {

   @Override
   public ExecutorService createExecutor(ThreadFactory factory) {
      return Executors.newCachedThreadPool(factory);
   }

}
