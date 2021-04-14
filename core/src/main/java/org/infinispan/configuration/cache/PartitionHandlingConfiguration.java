package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfiguration extends ConfigurationElement<PartitionHandlingConfiguration> {

   public static final AttributeDefinition<PartitionHandling> WHEN_SPLIT = AttributeDefinition.builder(Attribute.WHEN_SPLIT, PartitionHandling.ALLOW_READ_WRITES)
         .immutable().build();
   public static final AttributeDefinition<EntryMergePolicy> MERGE_POLICY = AttributeDefinition.builder(Attribute.MERGE_POLICY, MergePolicy.NONE, EntryMergePolicy.class)
         .immutable().build();


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PartitionHandlingConfiguration.class, WHEN_SPLIT, MERGE_POLICY);
   }

   public PartitionHandlingConfiguration(AttributeSet attributes) {
      super(Element.PARTITION_HANDLING, attributes);
   }

   public PartitionHandling whenSplit() {
      return attributes.attribute(WHEN_SPLIT).get();
   }

   public EntryMergePolicy mergePolicy() {
      return attributes.attribute(MERGE_POLICY).get();
   }

   public boolean resolveConflictsOnMerge() {
      EntryMergePolicy policy = mergePolicy();
      if (policy == MergePolicy.NONE)
         return false;

      return policy != null;
   }
}
