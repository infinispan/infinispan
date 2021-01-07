package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Determines whether cache statistics are gathered.
 *
 * @since 10.1.3
 */
public class StatisticsConfiguration extends ConfigurationElement<StatisticsConfiguration> implements JMXStatisticsConfiguration  {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STATISTICS, false).build();
   public static final AttributeDefinition<Boolean> AVAILABLE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STATISTICS_AVAILABLE, true).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StatisticsConfiguration.class, ENABLED, AVAILABLE);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Boolean> available;

   /**
    * Enable or disable statistics gathering.
    *
    * @param attributes
    */
   StatisticsConfiguration(AttributeSet attributes) {
      super(Element.JMX_STATISTICS, attributes);
      enabled = attributes.attribute(ENABLED);
      available = attributes.attribute(AVAILABLE);
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
}
