package org.infinispan.configuration.cache;

abstract class AbstractConfigurationChildBuilder implements ConfigurationChildBuilder {

   protected final ConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(ConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ConfigurationChildBuilder template(boolean template) {
      return builder.template(template);
   }

   public ConfigurationChildBuilder simpleCache(boolean simpleCache) {
      return builder.simpleCache(simpleCache);
   }

   @Override
   public boolean simpleCache() {
      return builder.simpleCache();
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
   public EncodingConfigurationBuilder encoding() {
      return builder.encoding();
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
   public PersistenceConfigurationBuilder persistence() {
      return builder.persistence();
   }

   @Override
   public LockingConfigurationBuilder locking() {
      return builder.locking();
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return builder.security();
   }

   @Override
   @Deprecated
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

   @Override
   public SitesConfigurationBuilder sites() {
      return builder.sites();
   }

   @Override
   @Deprecated
   public CompatibilityModeConfigurationBuilder compatibility() {
      return builder.compatibility();
   }

   @Override
   public MemoryConfigurationBuilder memory() {
      return builder.memory();
   }

   protected ConfigurationBuilder getBuilder() {
      return builder;
   }

   @Override
   public Configuration build() {
      return builder.build();
   }

}
