package org.infinispan.configuration.cache;

abstract class AbstractClusteringConfigurationChildBuilder extends AbstractConfigurationChildBuilder implements ClusteringConfigurationChildBuilder {

   private final ClusteringConfigurationBuilder clusteringBuilder;

   protected AbstractClusteringConfigurationChildBuilder(ClusteringConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.clusteringBuilder = builder;
   }

   @Override
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
   public StateTransferConfigurationBuilder stateTransfer() {
      return clusteringBuilder.stateTransfer();
   }

   @Override
   public SyncConfigurationBuilder sync() {
      return clusteringBuilder.sync();
   }

   @Override
   public PartitionHandlingConfigurationBuilder partitionHandling() {
      return clusteringBuilder.partitionHandling();
   }

   protected ClusteringConfigurationBuilder getClusteringBuilder() {
      return clusteringBuilder;
   }

}
