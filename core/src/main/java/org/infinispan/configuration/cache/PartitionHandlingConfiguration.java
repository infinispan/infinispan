package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfiguration implements Matchable<PartitionHandlingConfiguration> {

   @Deprecated
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable()
         .build();
   public static final AttributeDefinition<PartitionHandling> WHEN_SPLIT = AttributeDefinition.builder("whenSplit", PartitionHandling.ALLOW_READ_WRITES)
         .immutable().build();
   public static final AttributeDefinition<EntryMergePolicy> MERGE_POLICY = AttributeDefinition.builder("mergePolicy", MergePolicy.NONE, EntryMergePolicy.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PartitionHandlingConfiguration.class, ENABLED, WHEN_SPLIT, MERGE_POLICY);
   }

   private final AttributeSet attributes;

   public PartitionHandlingConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   /**
    * @deprecated Since 9.2, replaced with {@link #whenSplit()}.
    */
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

   public boolean resolveConflictsOnMerge() {
      EntryMergePolicy policy = mergePolicy();
      if (policy == MergePolicy.NONE)
         return false;

      return policy != null;
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
