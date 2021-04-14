package org.infinispan.configuration.global;

import java.util.List;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/*
 * @since 10.0
 */
public class StackConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StackConfiguration.class, NAME);
   }

   private final Attribute<String> name;
   private final List<JGroupsProtocolConfiguration> protocolConfigurations;
   private final AttributeSet attributes;

   StackConfiguration(AttributeSet attributes, List<JGroupsProtocolConfiguration> protocolConfigurations) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.protocolConfigurations = protocolConfigurations;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return name.get();
   }

   @Override
   public String toString() {
      return "StackConfiguration{" +
            "protocolConfigurations=" + protocolConfigurations +
            ", attributes=" + attributes +
            '}';
   }
}
