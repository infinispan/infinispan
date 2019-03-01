package org.infinispan.lock.impl;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.configuration.ClusteredLockManagerConfiguration;
import org.infinispan.lock.configuration.ClusteredLockManagerConfigurationBuilder;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.IsLocked;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.functions.UnlockFunction;
import org.infinispan.lock.impl.lock.ClusteredLockFilter;
import org.infinispan.lock.impl.manager.CacheHolder;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.lock.logging.Log;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.kohsuke.MetaInfServices;

/**
 * Locks module configuration
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@MetaInfServices(value = ModuleLifecycle.class)
public class ClusteredLockModuleLifecycle implements ModuleLifecycle {
   private static final Log log = LogFactory.getLog(ClusteredLockModuleLifecycle.class, Log.class);

   public static final String CLUSTERED_LOCK_CACHE_NAME = "org.infinispan.LOCKS";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();
      externalizerMap.put(ClusteredLockKey.EXTERNALIZER.getId(), ClusteredLockKey.EXTERNALIZER);
      externalizerMap.put(ClusteredLockValue.EXTERNALIZER.getId(), ClusteredLockValue.EXTERNALIZER);
      externalizerMap.put(LockFunction.EXTERNALIZER.getId(), LockFunction.EXTERNALIZER);
      externalizerMap.put(UnlockFunction.EXTERNALIZER.getId(), UnlockFunction.EXTERNALIZER);
      externalizerMap.put(IsLocked.EXTERNALIZER.getId(), IsLocked.EXTERNALIZER);
      externalizerMap.put(ClusteredLockFilter.EXTERNALIZER.getId(), ClusteredLockFilter.EXTERNALIZER);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // This works because locks are not yet used internally, otherwise it would have to be in cacheManagerStarting
      final EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      final InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);

      ClusteredLockManagerConfiguration config = extractConfiguration(gcr);
      GlobalConfiguration globalConfig = gcr.getGlobalConfiguration();

      if (globalConfig.isClustered()) {
         internalCacheRegistry.registerInternalCache(CLUSTERED_LOCK_CACHE_NAME, createClusteredLockCacheConfiguration(config, globalConfig),
               EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));
         CompletableFuture<CacheHolder> future = startCaches(cacheManager);
         registerClusteredLockManager(gcr, future, config);
      } else {
         log.configurationNotClustered();
      }
   }

   private static ClusteredLockManagerConfiguration extractConfiguration(GlobalComponentRegistry globalComponentRegistry) {
      ClusteredLockManagerConfiguration config = globalComponentRegistry.getGlobalConfiguration()
            .module(ClusteredLockManagerConfiguration.class);
      return config == null ? ClusteredLockManagerConfigurationBuilder.defaultConfiguration() : config;
   }

   private static Configuration createClusteredLockCacheConfiguration(ClusteredLockManagerConfiguration config, GlobalConfiguration globalConfig) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      if (config.numOwners() > 0) {
         builder.clustering().cacheMode(CacheMode.DIST_SYNC)
               .hash().numOwners(config.numOwners());
      } else {
         builder.clustering().cacheMode(CacheMode.REPL_SYNC);
         if (globalConfig.isZeroCapacityNode()) {
            log.warn("When the node is configured as a zero-capacity node, you need to specify the number of owners for the lock");
         }
      }

      if (config.reliability() == Reliability.CONSISTENT) {
         builder.clustering()
               .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
      } else {
         builder.clustering().partitionHandling().whenSplit(PartitionHandling.ALLOW_READ_WRITES);
      }
      return builder.build();
   }

   private static CompletableFuture<CacheHolder> startCaches(EmbeddedCacheManager cacheManager) {
      final CompletableFuture<CacheHolder> future = new CompletableFuture<>();
      new Thread(() -> {
         try {
            Cache<? extends ClusteredLockKey, ClusteredLockValue> locksCache = cacheManager.getCache(CLUSTERED_LOCK_CACHE_NAME);
            future.complete(
                  new CacheHolder(locksCache.getAdvancedCache()));
         } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
         }
      }).start();
      return future;
   }

   private static void registerClusteredLockManager(GlobalComponentRegistry registry,
                                                    CompletableFuture<CacheHolder> future,
                                                    ClusteredLockManagerConfiguration config) {
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (registry) {
         ClusteredLockManager clusteredLockManager = registry.getComponent(ClusteredLockManager.class);
         if (clusteredLockManager == null || !(clusteredLockManager instanceof EmbeddedClusteredLockManager)) {
            clusteredLockManager = new EmbeddedClusteredLockManager(future, config);
            registry.registerComponent(clusteredLockManager, ClusteredLockManager.class);
            //this start() is only invoked when the DefaultCacheManager.start() is invoked
            //it is invoked here again to force it to check the managed global components
            // and register them in the MBeanServer, if they are missing.
            registry.getComponent(CacheManagerJmxRegistration.class).start(); //HACK!
         }
      }
   }

}
