package org.infinispan.configuration.cache;

public interface ConfigurationChildBuilder {

   public ClusteringConfigurationBuilder clustering();
   
   public CustomInterceptorsConfigurationBuilder customInterceptors();
   
   public DataContainerConfigurationBuilder dataContainer();
   
   public DeadlockDetectionConfigurationBuilder deadlockDetection();
   
   public EvictionConfigurationBuilder eviction();
   
   public ExpirationConfigurationBuilder expiration();
   
   public IndexingConfigurationBuilder indexing();
   
   public InvocationBatchingConfigurationBuilder invocationBatching();
   
   public JMXStatisticsConfigurationBuilder jmxStatistics();
   
   public LoadersConfigurationBuilder loaders();
   
   public LockingConfigurationBuilder locking();
   
   public StoreAsBinaryConfigurationBuilder storeAsBinary();
   
   public TransactionConfigurationBuilder transaction();
  
   public UnsafeConfigurationBuilder unsafe();
   
   public Configuration build();
   
}
