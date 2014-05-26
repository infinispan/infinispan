package org.infinispan.configuration.cache;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.partionhandling.PartitionHandlingStrategy;
import org.infinispan.partionhandling.impl.DegradedModePartitionHandlingStrategy;

public class PartitionHandlingConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<PartitionHandlingConfiguration> {

   private PartitionHandlingStrategy partitionHandlingStrategy = new DegradedModePartitionHandlingStrategy();
   private boolean enabled;

   public PartitionHandlingConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   public PartitionHandlingConfigurationBuilder strategy(PartitionHandlingStrategy partitionHandlingStrategy) {
      this.partitionHandlingStrategy = partitionHandlingStrategy;
      return this;
   }

   public PartitionHandlingConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   public void validate() {
      if (enabled && partitionHandlingStrategy == null)
         throw new CacheConfigurationException("the 'strategy' should not be null if partition handling is enabled.");
   }

   @Override
   public PartitionHandlingConfiguration create() {
      return new PartitionHandlingConfiguration(partitionHandlingStrategy, enabled);
   }

   @Override
   public Builder<?> read(PartitionHandlingConfiguration template) {
      this.partitionHandlingStrategy = template.strategy();
      this.enabled = template.enabled();
      return this;
   }
}
