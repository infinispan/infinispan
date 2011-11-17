package org.infinispan.configuration.cache;

abstract class AbstractClusteringConfigurationChildBuilder<T> extends AbstractConfigurationChildBuilder<T> implements ClusteringConfigurationChildBuilder {

   private final ClusteringConfigurationBuilder clusteringBuilder;

   protected AbstractClusteringConfigurationChildBuilder(ClusteringConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.clusteringBuilder = builder;
   }

   public AsyncConfigurationBuilder async() {
      return clusteringBuilder.async();
   }
   
   @Override
   public HashConfigurationBuilder hash() {
      return clusteringBuilder.hash();
   }
   
   @Override
   public L1ConfigurationBuilder l1() {
      return clusteringBuilder.l1();
   }
   
   @Override
   public StateRetrievalConfigurationBuilder stateRetrieval() {
      return clusteringBuilder.stateRetrieval();
   }
   
   @Override
   public SyncConfigurationBuilder sync() {
      return clusteringBuilder.sync();
   }
   
   protected ClusteringConfigurationBuilder getClusteringBuilder() {
      return clusteringBuilder;
   }
   
}
