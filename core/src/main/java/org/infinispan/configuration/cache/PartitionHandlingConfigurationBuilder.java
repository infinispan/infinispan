package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.PartitionHandlingConfiguration.MERGE_POLICY;
import static org.infinispan.configuration.cache.PartitionHandlingConfiguration.WHEN_SPLIT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.PartitionHandling;
/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<PartitionHandlingConfiguration> {

   private final AttributeSet attributes;

   public PartitionHandlingConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      attributes = PartitionHandlingConfiguration.attributeDefinitionSet();
   }

   /**
    * @param enabled If {@code true}, partitions will enter degraded mode. If {@code false}, they will keep working independently.
    *
    * @deprecated Since 9.2, please use {@link #whenSplit(PartitionHandling)} instead.
    */
   @Deprecated
   public PartitionHandlingConfigurationBuilder enabled(boolean enabled) {
      whenSplit(enabled ? PartitionHandling.DENY_READ_WRITES : PartitionHandling.ALLOW_READ_WRITES);
      mergePolicy(MergePolicy.NONE);
      return this;
   }

   public PartitionHandlingConfigurationBuilder whenSplit(PartitionHandling partitionHandling) {
      attributes.attribute(WHEN_SPLIT).set(partitionHandling);
      return this;
   }

   public PartitionHandlingConfigurationBuilder mergePolicy(EntryMergePolicy mergePolicy) {
      attributes.attribute(MERGE_POLICY).set(mergePolicy);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public PartitionHandlingConfiguration create() {
      return new PartitionHandlingConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(PartitionHandlingConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
