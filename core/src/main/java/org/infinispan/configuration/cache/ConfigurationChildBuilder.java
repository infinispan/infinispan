package org.infinispan.configuration.cache;

import org.infinispan.configuration.global.GlobalConfiguration;

public interface ConfigurationChildBuilder {

   ConfigurationChildBuilder simpleCache(boolean simpleCache);

   boolean simpleCache();

   ClusteringConfigurationBuilder clustering();

   CustomInterceptorsConfigurationBuilder customInterceptors();

   DataContainerConfigurationBuilder dataContainer();

   DeadlockDetectionConfigurationBuilder deadlockDetection();

   EvictionConfigurationBuilder eviction();

   ExpirationConfigurationBuilder expiration();

   IndexingConfigurationBuilder indexing();

   InvocationBatchingConfigurationBuilder invocationBatching();

   JMXStatisticsConfigurationBuilder jmxStatistics();

   PersistenceConfigurationBuilder persistence();

   LockingConfigurationBuilder locking();

   SecurityConfigurationBuilder security();

   StoreAsBinaryConfigurationBuilder storeAsBinary();

   TransactionConfigurationBuilder transaction();

   VersioningConfigurationBuilder versioning();

   UnsafeConfigurationBuilder unsafe();

   SitesConfigurationBuilder sites();

   CompatibilityModeConfigurationBuilder compatibility();

   MemoryConfigurationBuilder memory();

   default ConfigurationChildBuilder template(boolean template) {
      return this;
   }

   void validate(GlobalConfiguration globalConfig);

   Configuration build();
}
