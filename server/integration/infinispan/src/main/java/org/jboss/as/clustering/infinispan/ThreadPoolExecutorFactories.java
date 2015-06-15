package org.jboss.as.clustering.infinispan;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.server.commons.concurrent.ManagedExecutorService;
import org.infinispan.server.commons.concurrent.ManagedScheduledExecutorService;

public final class ThreadPoolExecutorFactories {

   private ThreadPoolExecutorFactories() {
   }

   public static ThreadPoolExecutorFactory mkManagedExecutorFactory(Executor executor) {
      return new ManagedThreadPoolExecutorFactory(executor);
   }

   public static ThreadPoolExecutorFactory mkManagedScheduledExecutorFactory(ScheduledExecutorService executor) {
      return new ManagedThreadPoolScheduledExecutorFactory(executor);
   }

   private static final class ManagedThreadPoolExecutorFactory
         implements ThreadPoolExecutorFactory {
      private final Executor executor;
      public ManagedThreadPoolExecutorFactory(Executor executor) {
         this.executor = executor;
      }
      @Override public void validate() { }
      @Override
      public ManagedExecutorService createExecutor(ThreadFactory factory) {
         return new ManagedExecutorService(this.executor);
      }
   }

   private static final class ManagedThreadPoolScheduledExecutorFactory
         implements ThreadPoolExecutorFactory {
      private final ScheduledExecutorService executor;
      public ManagedThreadPoolScheduledExecutorFactory(ScheduledExecutorService executor) {
         this.executor = executor;
      }
      @Override public void validate() { }
      @Override
      public ManagedExecutorService createExecutor(ThreadFactory factory) {
         return new ManagedScheduledExecutorService(this.executor);
      }
   }

}
