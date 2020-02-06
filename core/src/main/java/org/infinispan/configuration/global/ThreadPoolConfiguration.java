package org.infinispan.configuration.global;

import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;

/**
 * @author Galder Zamarreño
 */
public class ThreadPoolConfiguration {

   private final String name;
   private final ThreadFactory threadFactory;
   private final ThreadPoolExecutorFactory threadPoolFactory;

   protected ThreadPoolConfiguration(String name, ThreadFactory threadFactory, ThreadPoolExecutorFactory threadPoolFactory) {
      this.name = name;
      this.threadFactory = threadFactory;
      this.threadPoolFactory = threadPoolFactory;
   }

   public <T extends ThreadPoolExecutorFactory> T threadPoolFactory() {
      return (T) threadPoolFactory;
   }

   public <T extends ThreadFactory> T threadFactory() {
      return (T) threadFactory;
   }

   public String name() {
      return name;
   }

   @Override
   public String toString() {
      return "ThreadPoolConfiguration{" +
            "name=" + name +
            ", threadFactory=" + threadFactory +
            ", threadPoolFactory=" + threadPoolFactory +
            '}';
   }

}
