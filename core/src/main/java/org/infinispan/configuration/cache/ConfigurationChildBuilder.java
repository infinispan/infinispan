package org.infinispan.configuration.cache;

public interface ConfigurationChildBuilder {

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
   
   StoreAsBinaryConfigurationBuilder storeAsBinary();
   
   TransactionConfigurationBuilder transaction();
   
   VersioningConfigurationBuilder versioning();
  
   UnsafeConfigurationBuilder unsafe();

   SitesConfigurationBuilder sites();

   CompatibilityModeConfigurationBuilder compatibility();

   Configuration build();
}
