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
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
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
import org.infinispan.counter.impl.persistence.PersistenceContextInitializerImpl;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.LogFactory;

/**
 * It register a {@link EmbeddedCounterManager} to each {@link EmbeddedCacheManager} started and starts the cache on it.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@InfinispanModule(name = "clustered-counter", requiredModules = "core")
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
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
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

   private static CounterManagerConfiguration extractConfiguration(GlobalConfiguration globalConfiguration) {
      CounterManagerConfiguration config = globalConfiguration.module(CounterManagerConfiguration.class);
      return config == null ? CounterManagerConfigurationBuilder.defaultConfiguration() : config;
   }

   private static void registerCounterManager(EmbeddedCacheManager cacheManager, BasicComponentRegistry registry) {
      if (trace)
         log.tracef("Registering counter manager.");
      EmbeddedCounterManager counterManager = new EmbeddedCounterManager(cacheManager);
      // This must happen before CacheManagerJmxRegistration starts
      registry.registerComponent(CounterManager.class, counterManager, true);
      if (cacheManager.getCacheManagerConfiguration().jmx().enabled()) {
         try {
            CacheManagerJmxRegistration jmxRegistration = registry.getComponent(CacheManagerJmxRegistration.class).running();
            jmxRegistration.registerMBean(counterManager);
         } catch (Exception e) {
            throw log.jmxRegistrationFailed(e);
         }
      }
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

      // Only required by GlobalMarshaller
      addAdvancedExternalizer(externalizerMap, ResetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CounterKeyFilter.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, ReadFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, InitializeCounterFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, AddFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CompareAndSwapFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CreateAndCASFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CreateAndAddFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveFunction.EXTERNALIZER);

      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      EmbeddedCacheManager cacheManager = bcr.getComponent(EmbeddedCacheManager.class).wired();
      InternalCacheRegistry internalCacheRegistry = bcr.getComponent(InternalCacheRegistry.class).running();

      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      CounterManagerConfiguration counterManagerConfiguration = extractConfiguration(globalConfiguration);
      if (gcr.getGlobalConfiguration().isClustered()) {
         //only attempts to create the caches if the cache manager is clustered.
         registerCounterCache(internalCacheRegistry, counterManagerConfiguration);
      } else {
         //local only cache manager.
         registerLocalCounterCache(internalCacheRegistry);
      }
      registerCounterManager(cacheManager, bcr);
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      if (COUNTER_CACHE_NAME.equals(cacheName) && configuration.clustering().cacheMode().isClustered()) {
         BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
         CounterInterceptor counterInterceptor = new CounterInterceptor();
         bcr.registerComponent(CounterInterceptor.class, counterInterceptor, true);
         bcr.addDynamicDependency(AsyncInterceptorChain.class.getName(), CounterInterceptor.class.getName());
         bcr.getComponent(AsyncInterceptorChain.class).wired()
            .addInterceptorAfter(counterInterceptor, EntryWrappingInterceptor.class);
      }
   }
}
