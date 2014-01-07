package org.infinispan.configuration.global;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;

import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public class ThreadPoolConfiguration {

   private final ThreadFactory threadFactory;
   private final ThreadPoolExecutorFactory threadPoolFactory;

   protected ThreadPoolConfiguration(ThreadFactory threadFactory, ThreadPoolExecutorFactory threadPoolFactory) {
      this.threadFactory = threadFactory;
      this.threadPoolFactory = threadPoolFactory;
   }

   public <T extends ThreadPoolExecutorFactory> T threadPoolFactory() {
      return (T) threadPoolFactory;
   }

   public <T extends ThreadFactory> T threadFactory() {
      return (T) threadFactory;
   }

   @Override
   public String toString() {
      return "ThreadPoolConfiguration{" +
            "threadFactory=" + threadFactory +
            ", threadPoolFactory=" + threadPoolFactory +
            '}';
   }

}
