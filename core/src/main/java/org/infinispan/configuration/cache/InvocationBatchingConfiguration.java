package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class InvocationBatchingConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(InvocationBatchingConfiguration.class, ENABLED);
   }
   private final AttributeSet attributes;

   InvocationBatchingConfiguration(AttributeSet attributes) {
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
      return "InvocationBatchingConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      InvocationBatchingConfiguration other = (InvocationBatchingConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
