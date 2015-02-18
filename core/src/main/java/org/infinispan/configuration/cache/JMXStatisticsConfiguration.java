package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Determines whether statistics are gather and reported.
 *
 * @author pmuir
 *
 */
public class JMXStatisticsConfiguration {

   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).build();
   static AttributeSet attributeSet() {
      return new AttributeSet(JMXStatisticsConfiguration.class, ENABLED);
   }
   private final AttributeSet attributes;

   /**
    * Enable or disable statistics gathering and reporting
    *
    * @param enabled
    */
   JMXStatisticsConfiguration(AttributeSet attributes) {
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
      return attributes.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      JMXStatisticsConfiguration other = (JMXStatisticsConfiguration) obj;
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
