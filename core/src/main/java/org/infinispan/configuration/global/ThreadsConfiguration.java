package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

public class ThreadsConfiguration implements ConfigurationInfo {

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

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.THREADS.getLocalName());
   private final List<ConfigurationInfo> children = new ArrayList<>();

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
                        ThreadPoolConfiguration transportThreadPool) {
      this.asyncThreadPool = asyncThreadPool;
      this.expirationThreadPool = expirationThreadPool;
      this.listenerThreadPool = listenerThreadPool;
      this.persistenceThreadPool = persistenceThreadPool;
      this.remoteThreadPool = remoteThreadPool;
      this.stateTransferThreadPool = stateTransferThreadPool;
      this.transportThreadPool = transportThreadPool;
      this.threadFactories.addAll(threadFactories);
      this.boundedThreadPools.addAll(boundedThreadPools);
      this.cachedThreadPools.addAll(cachedThreadPools);
      this.scheduledThreadPools.addAll(scheduledThreadPools);
      children.addAll(threadFactories);
      children.addAll(boundedThreadPools);
      children.addAll(cachedThreadPools);
      children.addAll(scheduledThreadPools);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return children;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   public ThreadPoolConfiguration stateTransferThreadPool() {
      return stateTransferThreadPool;
   }

   public ThreadPoolConfiguration transportThreadPool() {
      return transportThreadPool;
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
