package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<PartitionHandlingConfiguration> {

   private static Log log = LogFactory.getLog(PartitionHandlingConfigurationBuilder.class);

   private boolean enabled;

   public PartitionHandlingConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * @param enabled If {@code true}, partitions will enter degraded mode. If {@code false}, they will keep working independently.
    */
   public PartitionHandlingConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   public void validate() {
      if (enabled && clustering().cacheMode().isReplicated()) {
         log.warnPartitionHandlingForReplicatedCaches();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
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
