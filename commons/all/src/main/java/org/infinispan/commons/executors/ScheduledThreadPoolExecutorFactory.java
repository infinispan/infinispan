package org.infinispan.commons.executors;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.util.concurrent.LifecycleRejectedExecutionHandler;

/**
 * @author Galder Zamarreño
 */
public enum ScheduledThreadPoolExecutorFactory implements ThreadPoolExecutorFactory<ScheduledExecutorService> {

   INSTANCE;

   @Override
   public ScheduledExecutorService createExecutor(ThreadFactory factory) {
      ScheduledThreadPoolExecutor result = new ScheduledThreadPoolExecutor(1, factory,
            LifecycleRejectedExecutionHandler.getInstance());
      result.setRemoveOnCancelPolicy(true);
      return result;
   }

   @Override
   public void validate() {
      // No-op
   }

   public static ScheduledThreadPoolExecutorFactory create() {
      return INSTANCE;
   }
}
