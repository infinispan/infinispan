package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;

public class PartitionHandlingConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<PartitionHandlingConfiguration> {

   private boolean enabled;

   public PartitionHandlingConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   public PartitionHandlingConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   public void validate() {}

   @Override
   public PartitionHandlingConfiguration create() {
      return new PartitionHandlingConfiguration(enabled);
   }

   @Override
   public Builder<?> read(PartitionHandlingConfiguration template) {
      this.enabled = template.enabled();
      return this;
   }
}
