package org.infinispan.commons.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public class ScheduledThreadPoolExecutorFactory implements ThreadPoolExecutorFactory {

   @Override
   public ScheduledExecutorService createExecutor(ThreadFactory factory) {
      return Executors.newSingleThreadScheduledExecutor(factory);
   }

}
