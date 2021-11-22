package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;

/*
 * @since 10.0
 */
public class StackConfiguration implements NamedStackConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> EXTENDS = AttributeDefinition.builder(Attribute.EXTENDS, null, String.class).build();
   private final EmbeddedJGroupsChannelConfigurator configurator;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StackConfiguration.class, NAME, EXTENDS);
   }

   private final AttributeSet attributes;

   StackConfiguration(AttributeSet attributes, EmbeddedJGroupsChannelConfigurator configurator) {
      this.attributes = attributes.checkProtection();
      this.configurator = configurator;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String extend() {
      return attributes.attribute(EXTENDS).get();
   }

   public EmbeddedJGroupsChannelConfigurator configurator() {
      return configurator;
   }

   @Override
   public String toString() {
      return "StackConfiguration{" +
            "configurator=" + configurator +
            ", attributes=" + attributes +
            '}';
   }
}
