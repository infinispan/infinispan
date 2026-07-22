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
public class StatisticsConfiguration extends ConfigurationElement<StatisticsConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STATISTICS, false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StatisticsConfiguration.class, ENABLED);
   }

   private final Attribute<Boolean> enabled;
   private final HotKeysConfiguration hotKeysConfiguration;

   /**
    * Enable or disable statistics gathering.
    */
   StatisticsConfiguration(AttributeSet attributes, HotKeysConfiguration hotKeysConfiguration) {
      super(Element.JMX_STATISTICS, attributes);
      enabled = attributes.attribute(ENABLED);
      this.hotKeysConfiguration = hotKeysConfiguration;
   }

   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Returns the hot key tracking configuration for this cache.
    *
    * @return the hot keys sub-configuration
    */
   public HotKeysConfiguration hotKeys() {
      return hotKeysConfiguration;
   }
}
