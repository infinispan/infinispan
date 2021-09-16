package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;

public class ThreadsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ThreadsConfiguration> {

   private final ThreadPoolConfigurationBuilder asyncThreadPool;
   private final ThreadPoolConfigurationBuilder expirationThreadPool;
   private final ThreadPoolConfigurationBuilder listenerThreadPool;
   private final ThreadPoolConfigurationBuilder persistenceThreadPool;
   private final ThreadPoolConfigurationBuilder remoteCommandThreadPool;
   private final ThreadPoolConfigurationBuilder stateTransferThreadPool;
   private final ThreadPoolConfigurationBuilder transportThreadPool;
   private final ThreadPoolConfigurationBuilder nonBlockingThreadPool;
   private final ThreadPoolConfigurationBuilder blockingThreadPool;
   private List<ThreadFactoryConfigurationBuilder> threadFactoryBuilders = new ArrayList<>();
   private List<BoundedThreadPoolConfigurationBuilder> boundedThreadPoolBuilders = new ArrayList<>();
   private List<ScheduledThreadPoolConfigurationBuilder> scheduledThreadPoolBuilders = new ArrayList<>();
   private List<CachedThreadPoolConfigurationBuilder> cachedThreadPoolBuilders = new ArrayList<>();

   private final Map<String, ThreadFactoryConfigurationBuilder> threadFactoryByName = new HashMap<>();
   private final Map<String, ThreadPoolBuilderAdapter> threadPoolByName = new HashMap<>();

   ThreadsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.asyncThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.expirationThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.listenerThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.persistenceThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.stateTransferThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.remoteCommandThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.transportThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.nonBlockingThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.blockingThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
   }

   public ThreadFactoryConfigurationBuilder addThreadFactory(String name) {
      ThreadFactoryConfigurationBuilder threadFactoryConfigurationBuilder = new ThreadFactoryConfigurationBuilder(getGlobalConfig(), name);
      threadFactoryBuilders.add(threadFactoryConfigurationBuilder);
      threadFactoryByName.put(name, threadFactoryConfigurationBuilder);
      return threadFactoryConfigurationBuilder;
   }

   public BoundedThreadPoolConfigurationBuilder addBoundedThreadPool(String name) {
      BoundedThreadPoolConfigurationBuilder configurationBuilder = new BoundedThreadPoolConfigurationBuilder(getGlobalConfig(), name);
      boundedThreadPoolBuilders.add(configurationBuilder);
      threadPoolByName.put(name, configurationBuilder);
      return configurationBuilder;
   }

   public ScheduledThreadPoolConfigurationBuilder addScheduledThreadPool(String name) {
      ScheduledThreadPoolConfigurationBuilder configurationBuilder = new ScheduledThreadPoolConfigurationBuilder(getGlobalConfig(), name);
      scheduledThreadPoolBuilders.add(configurationBuilder);
      threadPoolByName.put(name, configurationBuilder);
      return configurationBuilder;
   }

   public CachedThreadPoolConfigurationBuilder addCachedThreadPool(String name) {
      CachedThreadPoolConfigurationBuilder configurationBuilder = new CachedThreadPoolConfigurationBuilder(getGlobalConfig(), name);
      cachedThreadPoolBuilders.add(configurationBuilder);
      threadPoolByName.put(name, configurationBuilder);
      return configurationBuilder;
   }

   @Override
   public ThreadPoolConfigurationBuilder asyncThreadPool() {
      return asyncThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder expirationThreadPool() {
      return expirationThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder listenerThreadPool() {
      return listenerThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder persistenceThreadPool() {
      return persistenceThreadPool;
   }

   public ThreadPoolConfigurationBuilder remoteCommandThreadPool() {
      return remoteCommandThreadPool;
   }

   /**
    * @deprecated Since 10.1, no longer used.
    */
   @Deprecated
   @Override
   public ThreadPoolConfigurationBuilder stateTransferThreadPool() {
      return stateTransferThreadPool;
   }

   /**
    * @deprecated Since 11.0, no longer used.
    */
   @Deprecated
   public ThreadPoolConfigurationBuilder transportThreadPool() {
      return transportThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder nonBlockingThreadPool() {
      return nonBlockingThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder blockingThreadPool() {
      return blockingThreadPool;
   }

   @Override
   public ThreadsConfigurationBuilder read(ThreadsConfiguration template) {
      this.asyncThreadPool.read(template.asyncThreadPool());
      this.expirationThreadPool.read(template.expirationThreadPool());
      this.listenerThreadPool.read(template.listenerThreadPool());
      this.persistenceThreadPool.read(template.persistenceThreadPool());
      this.remoteCommandThreadPool.read(template.remoteThreadPool());
      this.stateTransferThreadPool.read(template.stateTransferThreadPool());
      this.transportThreadPool.read(template.transportThreadPool());
      this.nonBlockingThreadPool.read(template.nonBlockingThreadPool());
      this.blockingThreadPool.read(template.blockingThreadPool());

      template.threadFactories().forEach(s -> threadFactoryBuilders.add(new ThreadFactoryConfigurationBuilder(getGlobalConfig(), s.name().get()).read(s)));
      template.boundedThreadPools().forEach(s -> boundedThreadPoolBuilders.add(new BoundedThreadPoolConfigurationBuilder(getGlobalConfig(), s.name()).read(s)));
      template.cachedThreadPools().forEach(s -> cachedThreadPoolBuilders.add(new CachedThreadPoolConfigurationBuilder(getGlobalConfig(), s.name()).read(s)));
      template.scheduledThreadPools().forEach(s -> scheduledThreadPoolBuilders.add(new ScheduledThreadPoolConfigurationBuilder(getGlobalConfig(), s.name()).read(s)));
      return this;
   }

   @Override
   public ThreadsConfiguration create() {
      List<ThreadFactoryConfiguration> threadFactoryConfigurations = threadFactoryBuilders
            .stream().map(ThreadFactoryConfigurationBuilder::create).collect(Collectors.toList());

      List<BoundedThreadPoolConfiguration> boundedThreadPoolConfigurations = boundedThreadPoolBuilders
            .stream().map(BoundedThreadPoolConfigurationBuilder::create).collect(Collectors.toList());

      List<ScheduledThreadPoolConfiguration> scheduledThreadPoolConfigurations = scheduledThreadPoolBuilders
            .stream().map(ScheduledThreadPoolConfigurationBuilder::create).collect(Collectors.toList());

      List<CachedThreadPoolConfiguration> cachedThreadPoolConfigurations = cachedThreadPoolBuilders
            .stream().map(CachedThreadPoolConfigurationBuilder::create).collect(Collectors.toList());

      return new ThreadsConfiguration(threadFactoryConfigurations, boundedThreadPoolConfigurations,
            cachedThreadPoolConfigurations, scheduledThreadPoolConfigurations,
            asyncThreadPool.create(),
            expirationThreadPool.create(),
            listenerThreadPool.create(),
            persistenceThreadPool.create(),
            remoteCommandThreadPool.create(),
            stateTransferThreadPool.create(),
            transportThreadPool.create(),
            nonBlockingThreadPool.create(),
            blockingThreadPool.create());
   }

   public ThreadsConfigurationBuilder nodeName(String value) {
      threadFactoryBuilders.forEach(tfb -> tfb.nodeName(value));
      return this;
   }

   public ThreadFactoryConfigurationBuilder getThreadFactory(String threadFactoryName) {
      return threadFactoryByName.get(threadFactoryName);
   }

   public ThreadPoolBuilderAdapter getThreadPool(String threadFactoryName) {
      return threadPoolByName.get(threadFactoryName);
   }


}
