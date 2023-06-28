package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

public class TracingConfiguration extends ConfigurationElement<TracingConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, true, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TracingConfiguration.class, ENABLED);
   }

   protected TracingConfiguration(AttributeSet attributes) {
      super(Element.TRACING, attributes);
   }

   /**
    * Whether tracing is enabled or disabled on the given cache.
    * This property can be used to enable or disable tracing at runtime.
    *
    * @return Whether the tracing is enabled on the given cache
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }
}
