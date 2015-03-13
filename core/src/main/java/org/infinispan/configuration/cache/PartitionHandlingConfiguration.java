package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfiguration {

   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable()
         .build();

   static AttributeSet attributeSet() {
      return new AttributeSet(PartitionHandlingConfiguration.class, ENABLED);
   }

   private final AttributeSet attributes;

   public PartitionHandlingConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   AttributeSet attributes() {
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
