package org.infinispan.counter.impl;

import static java.util.EnumSet.of;
import static org.infinispan.registry.InternalCacheRegistry.Flag.EXCLUSIVE;
import static org.infinispan.registry.InternalCacheRegistry.Flag.PERSISTENT;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.function.AddFunction;
import org.infinispan.counter.impl.function.CompareAndSwapFunction;
import org.infinispan.counter.impl.function.CreateAndAddFunction;
import org.infinispan.counter.impl.function.CreateAndCASFunction;
import org.infinispan.counter.impl.function.InitializeCounterFunction;
import org.infinispan.counter.impl.function.ReadFunction;
import org.infinispan.counter.impl.function.RemoveFunction;
import org.infinispan.counter.impl.function.ResetFunction;
import org.infinispan.counter.impl.interceptor.CounterInterceptor;
import org.infinispan.counter.impl.listener.CounterKeyFilter;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.impl.strong.StrongCounterKey;
import org.infinispan.counter.impl.weak.WeakCounterKey;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * It register a {@link EmbeddedCounterManager} to each {@link EmbeddedCacheManager} started and starts the cache on it.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@MetaInfServices(value = ModuleLifecycle.class)
public class CounterModuleLifecycle implements ModuleLifecycle {

   public static final String COUNTER_CACHE_NAME = "org.infinispan.COUNTER";
   private static final Log log = LogFactory.getLog(CounterModuleLifecycle.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

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

   private static Configuration createLocalCounterCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL)
            .l1().disable()
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
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

   private static void registerCounterManager(EmbeddedCacheManager cacheManager, BasicComponentRegistry registry) {
      if (trace)
         log.tracef("Registering counter manager.");
      EmbeddedCounterManager counterManager = new EmbeddedCounterManager(cacheManager);
      // This must happen before CacheManagerJmxRegistration starts
      registry.registerComponent(CounterManager.class, counterManager, true);
      registry.getComponent(CacheManagerJmxRegistration.class).running().registerMBean(counterManager);
   }

   private static void registerCounterCache(InternalCacheRegistry registry, CounterManagerConfiguration config) {
      registry.registerInternalCache(COUNTER_CACHE_NAME, createCounterCacheConfiguration(config),
            of(EXCLUSIVE, PERSISTENT));
   }

   private static void registerLocalCounterCache(InternalCacheRegistry registry) {
      registry.registerInternalCache(COUNTER_CACHE_NAME, createLocalCounterCacheConfiguration(),
            of(EXCLUSIVE, PERSISTENT));
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
      addAdvancedExternalizer(externalizerMap, CompareAndSwapFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CounterState.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CreateAndCASFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CreateAndAddFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveFunction.EXTERNALIZER);

      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      EmbeddedCacheManager cacheManager = bcr.getComponent(EmbeddedCacheManager.class).wired();
      InternalCacheRegistry internalCacheRegistry = bcr.getComponent(InternalCacheRegistry.class).running();

      CounterManagerConfiguration counterManagerConfiguration = extractConfiguration(gcr);
      if (gcr.getGlobalConfiguration().isClustered()) {
         //only attempts to create the caches if the cache manager is clustered.
         registerCounterCache(internalCacheRegistry, counterManagerConfiguration);
      } else {
         //local only cache manager.
         registerLocalCounterCache(internalCacheRegistry);
      }
      registerCounterManager(cacheManager, bcr);
   }
}
