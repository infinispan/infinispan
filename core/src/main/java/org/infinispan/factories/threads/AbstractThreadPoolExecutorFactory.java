package org.infinispan.factories.threads;

import java.util.concurrent.ExecutorService;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;

/**
 * Abstract {@link ThreadPoolExecutorFactory} that contains commons variables for configuring a thread pool
 * @author wburns
 */
public abstract class AbstractThreadPoolExecutorFactory<T extends ExecutorService> implements ThreadPoolExecutorFactory<T> {
   protected final int maxThreads;
   protected final int coreThreads;
   protected final int queueLength;
   protected final long keepAlive;

   protected AbstractThreadPoolExecutorFactory(int maxThreads, int coreThreads, int queueLength, long keepAlive) {
      this.maxThreads = maxThreads;
      this.coreThreads = coreThreads;
      this.queueLength = queueLength;
      this.keepAlive = keepAlive;
   }

   public int maxThreads() {
      return maxThreads;
   }

   public int coreThreads() {
      return coreThreads;
   }

   public int queueLength() {
      return queueLength;
   }

   public long keepAlive() {
      return keepAlive;
   }
}
