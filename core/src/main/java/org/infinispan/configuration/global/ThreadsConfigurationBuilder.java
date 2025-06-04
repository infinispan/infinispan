package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class ThreadsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ThreadsConfiguration> {

   private final ThreadPoolConfigurationBuilder asyncThreadPool;
   private final ThreadPoolConfigurationBuilder expirationThreadPool;
   private final ThreadPoolConfigurationBuilder listenerThreadPool;
   private final ThreadPoolConfigurationBuilder persistenceThreadPool;
   private final ThreadPoolConfigurationBuilder nonBlockingThreadPool;
   private final ThreadPoolConfigurationBuilder blockingThreadPool;
   private final List<ThreadFactoryConfigurationBuilder> threadFactoryBuilders = new ArrayList<>();
   private final List<BoundedThreadPoolConfigurationBuilder> boundedThreadPoolBuilders = new ArrayList<>();
   private final List<ScheduledThreadPoolConfigurationBuilder> scheduledThreadPoolBuilders = new ArrayList<>();
   private final List<CachedThreadPoolConfigurationBuilder> cachedThreadPoolBuilders = new ArrayList<>();

   private final Map<String, ThreadFactoryConfigurationBuilder> threadFactoryByName = new HashMap<>();
   private final Map<String, ThreadPoolBuilderAdapter> threadPoolByName = new HashMap<>();

   ThreadsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.asyncThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.expirationThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.listenerThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.persistenceThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.nonBlockingThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      this.blockingThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
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

   @Override
   public ThreadPoolConfigurationBuilder nonBlockingThreadPool() {
      return nonBlockingThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder blockingThreadPool() {
      return blockingThreadPool;
   }

   @Override
   public ThreadsConfigurationBuilder read(ThreadsConfiguration template, Combine combine) {
      this.asyncThreadPool.read(template.asyncThreadPool(), combine);
      this.expirationThreadPool.read(template.expirationThreadPool(), combine);
      this.listenerThreadPool.read(template.listenerThreadPool(), combine);
      this.persistenceThreadPool.read(template.persistenceThreadPool(), combine);
      this.nonBlockingThreadPool.read(template.nonBlockingThreadPool(), combine);
      this.blockingThreadPool.read(template.blockingThreadPool(), combine);

      template.threadFactories().forEach(s -> threadFactoryBuilders.add(new ThreadFactoryConfigurationBuilder(getGlobalConfig(), s.name().get()).read(s, combine)));
      template.boundedThreadPools().forEach(s -> boundedThreadPoolBuilders.add(new BoundedThreadPoolConfigurationBuilder(getGlobalConfig(), s.name()).read(s, combine)));
      template.cachedThreadPools().forEach(s -> cachedThreadPoolBuilders.add(new CachedThreadPoolConfigurationBuilder(getGlobalConfig(), s.name()).read(s, combine)));
      template.scheduledThreadPools().forEach(s -> scheduledThreadPoolBuilders.add(new ScheduledThreadPoolConfigurationBuilder(getGlobalConfig(), s.name()).read(s, combine)));
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
