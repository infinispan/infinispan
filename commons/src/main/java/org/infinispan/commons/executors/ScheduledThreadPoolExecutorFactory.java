package org.infinispan.commons.executors;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public class ScheduledThreadPoolExecutorFactory implements ThreadPoolExecutorFactory {

   private static final ScheduledThreadPoolExecutorFactory INSTANCE = new ScheduledThreadPoolExecutorFactory();

   private ScheduledThreadPoolExecutorFactory() {
      // singleton
   }

   @Override
   public ScheduledExecutorService createExecutor(ThreadFactory factory) {
      return Executors.newSingleThreadScheduledExecutor(factory);
   }

   @Override
   public void validate() {
      // No-op
   }

   public static ScheduledThreadPoolExecutorFactory create() {
      return INSTANCE;
   }

}
