package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Determines whether statistics are gather and reported.
 *
 * @author pmuir
 *
 */
public class JMXStatisticsConfiguration {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).build();
   public static final AttributeDefinition<Boolean> AVAILABLE = AttributeDefinition.builder("available", true).build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JMXStatisticsConfiguration.class, ENABLED, AVAILABLE);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Boolean> available;
   private final AttributeSet attributes;

   /**
    * Enable or disable statistics gathering and reporting
    *
    * @param attributes
    */
   JMXStatisticsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      available = attributes.attribute(AVAILABLE);
   }

   public boolean enabled() {
      return enabled.get();
   }

   /**
    * If set to false, statistics gathering cannot be enabled during runtime. Performance optimization.
    * @return
    */
   public boolean available() {
      return available.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "JMXStatisticsConfiguration [attributes=" + attributes + "]";
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
