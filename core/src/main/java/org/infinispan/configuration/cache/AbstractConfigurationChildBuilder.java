package org.infinispan.configuration.cache;

abstract class AbstractConfigurationChildBuilder<T> implements ConfigurationChildBuilder {
   
   private final ConfigurationBuilder builder;
   
   protected AbstractConfigurationChildBuilder(ConfigurationBuilder builder) {
      this.builder = builder;
   }
   
   @Override
   public ClusteringConfigurationBuilder clustering() {
      return builder.clustering();
   }
   
   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return builder.customInterceptors();
   }
   
   @Override
   public DataContainerConfigurationBuilder dataContainer() {
      return builder.dataContainer();
   }
   
   @Override
   public DeadlockDetectionConfigurationBuilder deadlockDetection() {
      return builder.deadlockDetection();
   }
   
   @Override
   public EvictionConfigurationBuilder eviction() {
      return builder.eviction();
   }
   
   @Override
   public ExpirationConfigurationBuilder expiration() {
      return builder.expiration();
   }
   
   @Override
   public IndexingConfigurationBuilder indexing() {
      return builder.indexing();
   }
   
   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return builder.invocationBatching();
   }
   
   @Override
   public JMXStatisticsConfigurationBuilder jmxStatistics() {
      return builder.jmxStatistics();
   }
   
   @Override
   public LoadersConfigurationBuilder loaders() {
      return builder.loaders();
   }
   
   @Override
   public LockingConfigurationBuilder locking() {
      return builder.locking();
   }
   
   @Override
   public StoreAsBinaryConfigurationBuilder storeAsBinary() {
      return builder.storeAsBinary();
   }
   
   @Override
   public TransactionConfigurationBuilder transaction() {
      return builder.transaction();
   }

   @Override
   public VersioningConfigurationBuilder versioning() {
     return builder.versioning();
   }
   
   @Override
   public UnsafeConfigurationBuilder unsafe() {
      return builder.unsafe();
   }
   
   protected ConfigurationBuilder getBuilder() {
      return builder;
   }

   public Configuration build() {
      return builder.build();
   }

   abstract void validate();
   
   abstract T create();
   
}