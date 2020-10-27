package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Determines whether cache statistics are gathered.
 *
 * @since 10.1.3
 */
public class StatisticsConfiguration extends JMXStatisticsConfiguration implements Matchable<StatisticsConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).xmlName("statistics").build();
   public static final AttributeDefinition<Boolean> AVAILABLE = AttributeDefinition.builder("available", true).xmlName("statistics-available").build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StatisticsConfiguration.class, ENABLED, AVAILABLE);
   }

   static final ElementDefinition<StatisticsConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>("", false);

   private final Attribute<Boolean> enabled;
   private final Attribute<Boolean> available;
   private final AttributeSet attributes;

   /**
    * Enable or disable statistics gathering.
    *
    * @param attributes
    */
   StatisticsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      available = attributes.attribute(AVAILABLE);
   }

   @Override
   public ElementDefinition<StatisticsConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public boolean enabled() {
      return enabled.get();
   }

   /**
    * If set to false, statistics gathering cannot be enabled during runtime. Performance optimization.
    *
    * @deprecated since 10.1.3. This method will be removed in a future version.
    */
   @Deprecated
   public boolean available() {
      return available.get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "StatisticsConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null || getClass() != obj.getClass())
         return false;
      StatisticsConfiguration other = (StatisticsConfiguration) obj;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
