package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

public class ShutdownConfiguration extends ConfigurationElement<ShutdownConfiguration> {
   static final AttributeDefinition<ShutdownHookBehavior> HOOK_BEHAVIOR = AttributeDefinition.builder(Attribute.SHUTDOWN_HOOK, ShutdownHookBehavior.DEFAULT).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ShutdownConfiguration.class, HOOK_BEHAVIOR);
   }

   ShutdownConfiguration(AttributeSet attributes) {
      super(Element.SHUTDOWN, attributes);
   }

   public ShutdownHookBehavior hookBehavior() {
      return attributes.attribute(HOOK_BEHAVIOR).get();
   }
}
