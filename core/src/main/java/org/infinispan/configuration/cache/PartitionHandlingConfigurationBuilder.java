package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PartitionHandlingConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<PartitionHandlingConfiguration> {

   private static Log log = LogFactory.getLog(PartitionHandlingConfigurationBuilder.class);

   private boolean enabled;

   public PartitionHandlingConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   public PartitionHandlingConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   public void validate() {
      if (clustering().cacheMode().isReplicated()) {
         log.warnPartitionHandlingForReplicatedCaches();
      }
   }

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
