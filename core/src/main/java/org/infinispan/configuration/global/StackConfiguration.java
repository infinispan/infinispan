package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

/*
 * @since 10.0
 */
public class StackConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StackConfiguration.class, NAME);
   }

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.STACK.getLocalName());

   private final Attribute<String> name;
   private final List<JGroupsProtocolConfiguration> protocolConfigurations;
   private final AttributeSet attributes;
   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   StackConfiguration(AttributeSet attributes, List<JGroupsProtocolConfiguration> protocolConfigurations) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.protocolConfigurations = protocolConfigurations;
      this.subElements.addAll(protocolConfigurations);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
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
