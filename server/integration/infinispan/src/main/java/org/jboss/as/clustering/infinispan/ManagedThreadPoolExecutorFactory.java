package org.jboss.as.clustering.infinispan;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.jboss.as.clustering.concurrent.ManagedExecutorService;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public final class ManagedThreadPoolExecutorFactory implements ThreadPoolExecutorFactory {

   private final Executor executor;

   public ManagedThreadPoolExecutorFactory(Executor executor) {
      this.executor = executor;
   }

   @Override
   public ManagedExecutorService createExecutor(ThreadFactory factory) {
      return new ManagedExecutorService(this.executor);
   }

   @Override
   public void validate() {
      // No-op
   }

}
