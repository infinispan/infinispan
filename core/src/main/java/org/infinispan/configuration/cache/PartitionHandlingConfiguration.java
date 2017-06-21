package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfiguration {

   @Deprecated
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable()
         .build();
   public static final AttributeDefinition<PartitionHandling> WHEN_SPLIT = AttributeDefinition.builder("whenSplit", PartitionHandling.ALLOW_READ_WRITES)
         .immutable().build();
   public static final AttributeDefinition<EntryMergePolicy> MERGE_POLICY = AttributeDefinition.builder("mergePolicy",
         null, EntryMergePolicy.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PartitionHandlingConfiguration.class, ENABLED, WHEN_SPLIT, MERGE_POLICY);
   }

   private final AttributeSet attributes;

   public PartitionHandlingConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Deprecated
   public boolean enabled() {
      return whenSplit() != PartitionHandling.ALLOW_READ_WRITES;
   }

   public PartitionHandling whenSplit() {
      return attributes.attribute(WHEN_SPLIT).get();
   }

   public EntryMergePolicy mergePolicy() {
      return attributes.attribute(MERGE_POLICY).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "PartitionHandlingConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      PartitionHandlingConfiguration other = (PartitionHandlingConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }
}
