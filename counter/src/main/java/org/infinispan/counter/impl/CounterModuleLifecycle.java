package org.infinispan.counter.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.function.AddFunction;
import org.infinispan.counter.impl.function.CompareAndSetFunction;
import org.infinispan.counter.impl.function.CreateAndAddFunction;
import org.infinispan.counter.impl.function.CreateAndCASFunction;
import org.infinispan.counter.impl.function.InitializeCounterFunction;
import org.infinispan.counter.impl.function.ReadFunction;
import org.infinispan.counter.impl.function.RemoveFunction;
import org.infinispan.counter.impl.function.ResetFunction;
import org.infinispan.counter.impl.interceptor.CounterConfigurationInterceptor;
import org.infinispan.counter.impl.interceptor.CounterInterceptor;
import org.infinispan.counter.impl.listener.CounterKeyFilter;
import org.infinispan.counter.impl.manager.CacheHolder;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.impl.strong.StrongCounterKey;
import org.infinispan.counter.impl.weak.WeakCounterKey;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.kohsuke.MetaInfServices;

/**
 * It register a {@link EmbeddedCounterManager} to each {@link EmbeddedCacheManager} started and starts the cache on it.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@MetaInfServices(value = ModuleLifecycle.class)
public class CounterModuleLifecycle implements ModuleLifecycle {

   public static final String COUNTER_CACHE_NAME = "___counters";
   public static final String COUNTER_CONFIGURATION_CACHE_NAME = "___counter_configuration";

   private static Configuration createCounterCacheConfiguration(CounterManagerConfiguration config) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(config.numOwners())
            .stateTransfer().fetchInMemoryState(true)
            .l1().disable()
            .partitionHandling().whenSplit(config.reliability() == Reliability.CONSISTENT ?
                                           PartitionHandling.DENY_READ_WRITES :
                                           PartitionHandling.ALLOW_READ_WRITES)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .customInterceptors().addInterceptor().after(EntryWrappingInterceptor.class)
            .interceptor(new CounterInterceptor());
      return builder.build();
   }

   private static Configuration createCounterConfigurationCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
            .l1().disable()
            .stateTransfer().fetchInMemoryState(true)
            .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .customInterceptors().addInterceptor().after(EntryWrappingInterceptor.class)
            .interceptor(new CounterConfigurationInterceptor());
      return builder.build();
   }

   private static void addAdvancedExternalizer(Map<Integer, AdvancedExternalizer<?>> map, AdvancedExternalizer<?> ext) {
      map.put(ext.getId(), ext);
   }

   private static CounterManagerConfiguration extractConfiguration(GlobalComponentRegistry globalComponentRegistry) {
      CounterManagerConfiguration config = globalComponentRegistry.getGlobalConfiguration()
            .module(CounterManagerConfiguration.class);
      return config == null ? CounterManagerConfigurationBuilder.defaultConfiguration() : config;
   }

   private static void registerCounterManager(GlobalComponentRegistry registry, CompletableFuture<CacheHolder> future) {
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (registry) {
         CounterManager counterManager = registry.getComponent(CounterManager.class);
         if (counterManager == null || !(counterManager instanceof EmbeddedCounterManager)) {
            counterManager = new EmbeddedCounterManager(future,
                  registry.getGlobalConfiguration().globalState().enabled());
            registry.registerComponent(counterManager, CounterManager.class);
            //this start() is only invoked when the DefaultCacheManager.start() is invoked
            //it is invoked here again to force it to check the managed global components
            // and register them in the MBeanServer, if they are missing.
            registry.getComponent(CacheManagerJmxRegistration.class).start(); //HACK!
         }
      }
   }

   private static void registerCounterCache(InternalCacheRegistry registry, CounterManagerConfiguration config) {
      registry.registerInternalCache(COUNTER_CACHE_NAME, createCounterCacheConfiguration(config),
            EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE, InternalCacheRegistry.Flag.PERSISTENT));
   }

   private static void registerConfigurationCache(InternalCacheRegistry registry) {
      registry.registerInternalCache(COUNTER_CONFIGURATION_CACHE_NAME, createCounterConfigurationCacheConfiguration(),
            EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE, InternalCacheRegistry.Flag.PERSISTENT));
   }

   private static CompletableFuture<CacheHolder> startCaches(EmbeddedCacheManager cacheManager,
         List<AbstractCounterConfiguration> defaultCounters) {
      final CompletableFuture<CacheHolder> future = new CompletableFuture<>();
      new Thread(() -> {
         try {
            Cache<String, CounterConfiguration> configCache = cacheManager.getCache(COUNTER_CONFIGURATION_CACHE_NAME);
            Cache<? extends CounterKey, CounterValue> counterCache = cacheManager.getCache(COUNTER_CACHE_NAME);
            future.complete(
                  new CacheHolder(configCache.getAdvancedCache(), counterCache.getAdvancedCache(), defaultCounters));
         } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
         }
      }).start();
      return future;
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();

      addAdvancedExternalizer(externalizerMap, ResetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CounterKeyFilter.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, StrongCounterKey.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, WeakCounterKey.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, ReadFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CounterConfiguration.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CounterValue.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, InitializeCounterFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, ConfigurationMetadata.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, AddFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CompareAndSetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CounterState.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CreateAndCASFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CreateAndAddFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveFunction.EXTERNALIZER);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      final EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      final InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      final CounterManagerConfiguration counterManagerConfiguration = extractConfiguration(gcr);

      registerCounterCache(internalCacheRegistry, counterManagerConfiguration);
      registerConfigurationCache(internalCacheRegistry);

      CompletableFuture<CacheHolder> future = startCaches(cacheManager, counterManagerConfiguration.counters());
      registerCounterManager(gcr, future);
   }

}
