package org.infinispan.commons.util.concurrent;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A handler for rejected tasks that runs the rejected task
 * directly in the calling thread of the {@code execute} method,
 * even if the executor has been shutdown.
 */
public class CallerAlwaysRunsPolicy implements RejectedExecutionHandler {
   @Override
   public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      r.run();
   }
}
