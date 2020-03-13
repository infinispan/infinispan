package org.infinispan.commons.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.executors.NonBlockingThread;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * A handler for rejected tasks that runs the task if the current thread is a blocking thread otherwise it
 * rejects the task.
 * @author wburns
 * @since 10.1
 */
public class BlockingRejectedExecutionHandler implements RejectedExecutionHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private BlockingRejectedExecutionHandler() { }

   private static final BlockingRejectedExecutionHandler INSTANCE = new BlockingRejectedExecutionHandler();

   public static RejectedExecutionHandler getInstance() {
      return INSTANCE;
   }

   @Override
   public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
         throw new IllegalLifecycleStateException();
      }
      if (Thread.currentThread() instanceof NonBlockingThread) {
         if (trace) {
            log.tracef("Task %s was rejected from %s when submitted from non blocking thread", r, executor);
         }
         throw new CacheBackpressureFullException();
      } else {
         r.run();
      }
   }
}
