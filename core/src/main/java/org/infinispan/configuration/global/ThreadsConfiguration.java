package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.List;

public class ThreadsConfiguration {

   private final List<ThreadFactoryConfiguration> threadFactories = new ArrayList<>();
   private final List<BoundedThreadPoolConfiguration> boundedThreadPools = new ArrayList<>();
   private final List<CachedThreadPoolConfiguration> cachedThreadPools = new ArrayList<>();
   private final List<ScheduledThreadPoolConfiguration> scheduledThreadPools = new ArrayList<>();
   private final ThreadPoolConfiguration asyncThreadPool;
   private final ThreadPoolConfiguration expirationThreadPool;
   private final ThreadPoolConfiguration listenerThreadPool;
   private final ThreadPoolConfiguration persistenceThreadPool;
   private final ThreadPoolConfiguration remoteThreadPool;
   private final ThreadPoolConfiguration stateTransferThreadPool;
   private final ThreadPoolConfiguration transportThreadPool;
   private final ThreadPoolConfiguration nonBlockingThreadPool;
   private final ThreadPoolConfiguration blockingThreadPool;

   ThreadsConfiguration(List<ThreadFactoryConfiguration> threadFactories,
                        List<BoundedThreadPoolConfiguration> boundedThreadPools,
                        List<CachedThreadPoolConfiguration> cachedThreadPools,
                        List<ScheduledThreadPoolConfiguration> scheduledThreadPools,
                        ThreadPoolConfiguration asyncThreadPool,
                        ThreadPoolConfiguration expirationThreadPool,
                        ThreadPoolConfiguration listenerThreadPool,
                        ThreadPoolConfiguration persistenceThreadPool,
                        ThreadPoolConfiguration remoteThreadPool,
                        ThreadPoolConfiguration stateTransferThreadPool,
                        ThreadPoolConfiguration transportThreadPool,
                        ThreadPoolConfiguration nonBlockingThreadPool,
                        ThreadPoolConfiguration blockingThreadPool) {
      this.asyncThreadPool = asyncThreadPool;
      this.expirationThreadPool = expirationThreadPool;
      this.listenerThreadPool = listenerThreadPool;
      this.persistenceThreadPool = persistenceThreadPool;
      this.remoteThreadPool = remoteThreadPool;
      this.stateTransferThreadPool = stateTransferThreadPool;
      this.transportThreadPool = transportThreadPool;
      this.nonBlockingThreadPool = nonBlockingThreadPool;
      this.blockingThreadPool = blockingThreadPool;
      this.threadFactories.addAll(threadFactories);
      this.boundedThreadPools.addAll(boundedThreadPools);
      this.cachedThreadPools.addAll(cachedThreadPools);
      this.scheduledThreadPools.addAll(scheduledThreadPools);
   }

   public ThreadPoolConfiguration asyncThreadPool() {
      return asyncThreadPool;
   }

   public ThreadPoolConfiguration expirationThreadPool() {
      return expirationThreadPool;
   }

   public ThreadPoolConfiguration listenerThreadPool() {
      return listenerThreadPool;
   }

   public ThreadPoolConfiguration persistenceThreadPool() {
      return persistenceThreadPool;
   }

   public ThreadPoolConfiguration remoteThreadPool() {
      return remoteThreadPool;
   }

   /**
    * @return An empty {@code ThreadPoolConfiguration}.
    * @deprecated Since 10.1, no longer used.
    */
   @Deprecated
   public ThreadPoolConfiguration stateTransferThreadPool() {
      return stateTransferThreadPool;
   }

   /**
    * @return An empty {@code ThreadPoolConfiguration}.
    * @deprecated Since 11.0, no longer used.
    */
   public ThreadPoolConfiguration transportThreadPool() {
      return transportThreadPool;
   }

   public ThreadPoolConfiguration nonBlockingThreadPool() {
      return nonBlockingThreadPool;
   }

   public ThreadPoolConfiguration blockingThreadPool() {
      return blockingThreadPool;
   }

   public List<ThreadFactoryConfiguration> threadFactories() {
      return threadFactories;
   }

   public List<BoundedThreadPoolConfiguration> boundedThreadPools() {
      return boundedThreadPools;
   }

   public List<CachedThreadPoolConfiguration> cachedThreadPools() {
      return cachedThreadPools;
   }

   public List<ScheduledThreadPoolConfiguration> scheduledThreadPools() {
      return scheduledThreadPools;
   }

   @Override
   public String toString() {
      return "ThreadsConfiguration{" +
            "threadFactories=" + threadFactories +
            ", boundedThreadPools=" + boundedThreadPools +
            ", cachedThreadPools=" + cachedThreadPools +
            ", scheduledThreadPools=" + scheduledThreadPools +
            ", asyncThreadPool=" + asyncThreadPool +
            ", expirationThreadPool=" + expirationThreadPool +
            ", listenerThreadPool=" + listenerThreadPool +
            ", persistenceThreadPool=" + persistenceThreadPool +
            ", remoteThreadPool=" + remoteThreadPool +
            ", stateTransferThreadPool=" + stateTransferThreadPool +
            ", transportThreadPool=" + transportThreadPool +
            '}';
   }
}
