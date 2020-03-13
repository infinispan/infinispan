package org.infinispan.commons.util.concurrent;

import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.commons.IllegalLifecycleStateException;

/**
 * A handler for rejected tasks that always throws a {@link IllegalLifecycleStateException}.
 * @author wburns
 * @since 11.0
 */
public class LifecycleRejectedExecutionHandler extends ThreadPoolExecutor.AbortPolicy {
   private LifecycleRejectedExecutionHandler() { }

   private static final LifecycleRejectedExecutionHandler INSTANCE = new LifecycleRejectedExecutionHandler();

   public static LifecycleRejectedExecutionHandler getInstance() {
      return INSTANCE;
   }

   @Override
   public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
         throw new IllegalLifecycleStateException();
      }
      super.rejectedExecution(r, executor);
   }
}
