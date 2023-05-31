package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.PartitionHandlingConfiguration.MERGE_POLICY;
import static org.infinispan.configuration.cache.PartitionHandlingConfiguration.WHEN_SPLIT;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.conflict.EntryMergePolicy;
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

   PartitionHandling whenSplit() {
      return attributes.attribute(WHEN_SPLIT).get();
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
      if (attributes.attribute(WHEN_SPLIT).get() != PartitionHandling.ALLOW_READ_WRITES && clustering().cacheMode().isInvalidation())
         throw CONFIG.invalidationPartitionHandlingNotSuported();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public PartitionHandlingConfiguration create() {
      return new PartitionHandlingConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(PartitionHandlingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
