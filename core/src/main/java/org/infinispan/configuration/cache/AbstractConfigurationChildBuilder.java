package org.infinispan.configuration.cache;

abstract class AbstractConfigurationChildBuilder implements ConfigurationChildBuilder {

   protected final ConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(ConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ConfigurationBuilder aliases(String... aliases) {
      return builder.aliases(aliases);
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

   public EncodingConfigurationBuilder encoding() {
      return builder.encoding();
   }

   @Override
   public ExpirationConfigurationBuilder expiration() {
      return builder.expiration();
   }

   @Override
   public QueryConfigurationBuilder query() {
      return builder.query();
   }

   @Override
   public IndexingConfigurationBuilder indexing() {
      return builder.indexing();
   }

   @Override
   public TracingConfigurationBuilder tracing() {
      return builder.tracing();
   }

   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return builder.invocationBatching();
   }

   @Override
   public StatisticsConfigurationBuilder statistics() {
      return builder.statistics();
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
   public TransactionConfigurationBuilder transaction() {
      return builder.transaction();
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
