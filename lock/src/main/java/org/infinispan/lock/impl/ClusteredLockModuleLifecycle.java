package org.infinispan.lock.impl;

import java.util.EnumSet;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.configuration.ClusteredLockManagerConfiguration;
import org.infinispan.lock.configuration.ClusteredLockManagerConfigurationBuilder;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.lock.logging.Log;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;

/**
 * Locks module configuration
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@InfinispanModule(name = "clustered-lock", requiredModules = "core")
public class ClusteredLockModuleLifecycle implements ModuleLifecycle {
   private static final Log log = LogFactory.getLog(ClusteredLockModuleLifecycle.class, Log.class);

   public static final String CLUSTERED_LOCK_CACHE_NAME = "org.infinispan.LOCKS";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      if (!globalConfiguration.isClustered()) {
         log.configurationNotClustered();
         return;
      }

      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new GlobalContextInitializerImpl());

      ClusteredLockManagerConfiguration config = extractConfiguration(gcr);
      InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      Configuration lockConfig = createClusteredLockCacheConfiguration(config, globalConfiguration);
      internalCacheRegistry.registerInternalCache(CLUSTERED_LOCK_CACHE_NAME, lockConfig, EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));
      registerClusteredLockManager(gcr.getComponent(BasicComponentRegistry.class), globalConfiguration, config);
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

      // If numOwners = 1, we can't use DENY_READ_WRITES as a single node leaving will cause the cluster to become DEGRADED
      int numOwners = config.numOwners() < 0 ? HashConfiguration.NUM_OWNERS.getDefaultValue() : config.numOwners();
      if (config.reliability() == Reliability.CONSISTENT && numOwners > 1) {
         builder.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
      } else {
         builder.clustering().partitionHandling().whenSplit(PartitionHandling.ALLOW_READ_WRITES);
      }
      return builder.build();
   }

   private static void registerClusteredLockManager(BasicComponentRegistry registry,
                                                    GlobalConfiguration globalConfig,
                                                    ClusteredLockManagerConfiguration config) {
      ClusteredLockManager clusteredLockManager = new EmbeddedClusteredLockManager(config);
      registry.registerComponent(ClusteredLockManager.class, clusteredLockManager, true);

      if (globalConfig.jmx().enabled()) {
         try {
            CacheManagerJmxRegistration jmxRegistration = registry.getComponent(CacheManagerJmxRegistration.class).running();
            jmxRegistration.registerMBean(clusteredLockManager);
         } catch (Exception e) {
            throw log.jmxRegistrationFailed(e);
         }
      }
   }
}
