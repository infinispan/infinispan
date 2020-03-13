package org.infinispan.commons.util.concurrent;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.commons.IllegalLifecycleStateException;

/**
 * A handler for rejected tasks that runs the rejected task
 * directly in the calling thread of the {@code execute} method. If
 * the executor was shutdown, it will instead throw a {@link RejectedExecutionException}.
 * @author wburns
 * @since 10.0
 */
public class CallerRunsRejectOnShutdownPolicy extends ThreadPoolExecutor.AbortPolicy {
   @Override
   public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
         throw new IllegalLifecycleStateException();
      }
      r.run();
   }
}
