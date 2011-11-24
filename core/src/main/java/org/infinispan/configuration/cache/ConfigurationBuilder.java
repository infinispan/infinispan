package org.infinispan.configuration.cache;

public class ConfigurationBuilder implements ConfigurationChildBuilder {

   private String name;
   private final ClusteringConfigurationBuilder clustering;
   private final CustomInterceptorsConfigurationBuilder customInterceptors;
   private final DataContainerConfigurationBuilder dataContainer;
   private final DeadlockDetectionConfigurationBuilder deadlockDetection;
   private final EvictionConfigurationBuilder eviction;
   private final ExpirationConfigurationBuilder expiration;
   private final IndexingConfigurationBuilder indexing;
   private final InvocationBatchingConfigurationBuilder invocationBatching;
   private final JMXStatisticsConfigurationBuilder jmxStatistics;
   private final LoadersConfigurationBuilder loaders;
   private final LockingConfigurationBuilder locking;
   private final StoreAsBinaryConfigurationBuilder storeAsBinary;
   private final TransactionConfigurationBuilder transaction;
   private final UnsafeConfigurationBuilder unsafe;
   
   public ConfigurationBuilder() {
      this.clustering = new ClusteringConfigurationBuilder(this);
      this.customInterceptors = new CustomInterceptorsConfigurationBuilder(this);
      this.dataContainer = new DataContainerConfigurationBuilder(this);
      this.deadlockDetection = new DeadlockDetectionConfigurationBuilder(this);
      this.eviction = new EvictionConfigurationBuilder(this);
      this.expiration = new ExpirationConfigurationBuilder(this);
      this.indexing = new IndexingConfigurationBuilder(this);
      this.invocationBatching = new InvocationBatchingConfigurationBuilder(this);
      this.jmxStatistics = new JMXStatisticsConfigurationBuilder(this);
      this.loaders = new LoadersConfigurationBuilder(this);
      this.locking = new LockingConfigurationBuilder(this);
      this.storeAsBinary = new StoreAsBinaryConfigurationBuilder(this);
      this.transaction = new TransactionConfigurationBuilder(this);
      this.unsafe = new UnsafeConfigurationBuilder(this);
   }
   
   public ConfigurationBuilder name(String name) {
      this.name = name;
      return this;
   }

   @Override
   public ClusteringConfigurationBuilder clustering() {
      return clustering;
   }
   
   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return customInterceptors;
   }
   
   @Override
   public DataContainerConfigurationBuilder dataContainer() {
      return dataContainer;
   } 
   
   @Override
   public DeadlockDetectionConfigurationBuilder deadlockDetection() {
      return deadlockDetection;
   }
   
   @Override
   public EvictionConfigurationBuilder eviction() {
      return eviction;
   }
   
   @Override
   public ExpirationConfigurationBuilder expiration() {
      return expiration;
   }
   
   @Override
   public IndexingConfigurationBuilder indexing() {
      return indexing;
   }
   
   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return invocationBatching;
   }
   
   @Override
   public JMXStatisticsConfigurationBuilder jmxStatistics() {
      return jmxStatistics;
   }
   
   @Override
   public StoreAsBinaryConfigurationBuilder storeAsBinary() {
      return storeAsBinary;
   }
   
   @Override
   public LoadersConfigurationBuilder loaders() {
      return loaders;
   }
   
   @Override
   public LockingConfigurationBuilder locking() {
      return locking;
   }
   
   @Override
   public TransactionConfigurationBuilder transaction() {
      return transaction;
   }
   
   @Override
   public UnsafeConfigurationBuilder unsafe() {
      return unsafe;
   }
   
   public void validate() {
      clustering.validate();
      dataContainer.validate();
      deadlockDetection.validate();
      eviction.validate();
      expiration.validate();
      indexing.validate();
      invocationBatching.validate();
      jmxStatistics.validate();
      loaders.validate();
      locking.validate();
      storeAsBinary.validate();
      transaction.validate();
      unsafe.validate();
   }

   @Override
   public Configuration build() {
      validate();
      return new Configuration(name,
            clustering.create(),
            customInterceptors.create(),
            dataContainer.create(),
            deadlockDetection.create(),
            eviction.create(),
            expiration.create(),
            indexing.create(),
            invocationBatching.create(),
            jmxStatistics.create(),
            loaders.create(),
            locking.create(),
            storeAsBinary.create(),
            transaction.create(),
            unsafe.create(), null );// TODO
   }

   
   
}
