package org.infinispan.commons.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.executors.NonBlockingThread;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * A handler for rejected tasks that runs the task if the current thread is a non blocking thread otherwise it
 * blocks until the task can be added to the underlying queue
 * @author wburns
 * @since 10.1
 */
public class NonBlockingRejectedExecutionHandler implements RejectedExecutionHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private NonBlockingRejectedExecutionHandler() { }

   private static final NonBlockingRejectedExecutionHandler INSTANCE = new NonBlockingRejectedExecutionHandler();

   public static RejectedExecutionHandler getInstance() {
      return INSTANCE;
   }

   @Override
   public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
         throw new IllegalLifecycleStateException();
      }
      Thread currentThread = Thread.currentThread();
      if (currentThread instanceof NonBlockingThread) {
         r.run();
      } else {
         if (trace) {
            log.tracef("Current thread will wait until task %s is placed into the queue of %s", r, executor);
         }
         try {
            executor.getQueue().put(r);
         } catch (InterruptedException e) {
            currentThread.interrupt();
            throw new CacheException(e);
         }
      }
   }
}
