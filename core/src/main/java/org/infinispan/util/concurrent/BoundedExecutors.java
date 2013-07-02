package org.infinispan.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Similar to JDK {@link java.util.concurrent.Executors} except that the factory methods here allow you to specify the
 * size of the blocking queue that backs the executor.
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class BoundedExecutors {
   /**
    * Creates a thread pool that reuses a fixed set of threads operating off a shared bounded queue. If any thread
    * terminates due to a failure during execution prior to shutdown, a new one will take its place if needed to execute
    * subsequent tasks.
    *
    * @param nThreads         the number of threads in the pool
    * @param boundedQueueSize size of the bounded queue
    * @return the newly created thread pool
    */
   public static ExecutorService newFixedThreadPool(int nThreads, int boundedQueueSize) {
      return new ThreadPoolExecutor(nThreads, nThreads,
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(boundedQueueSize));
   }

   /**
    * Creates a thread pool that reuses a fixed set of threads operating off a shared bounded queue, using the provided
    * ThreadFactory to create new threads when needed.
    *
    * @param nThreads         the number of threads in the pool
    * @param threadFactory    the factory to use when creating new threads
    * @param boundedQueueSize size of the bounded queue
    * @return the newly created thread pool
    */
   public static ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory, int boundedQueueSize) {
      return new ThreadPoolExecutor(nThreads, nThreads,
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(boundedQueueSize),
                                    threadFactory);
   }
}
