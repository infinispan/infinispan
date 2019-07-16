package org.infinispan.configuration.global;

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
public class StackFileConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StackFileConfiguration.class, NAME, PATH);
   }

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.STACK_FILE.getLocalName());

   private final Attribute<String> name;
   private final Attribute<String> path;
   private final AttributeSet attributes;

   StackFileConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.path = attributes.attribute(PATH);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return name.get();
   }

   public String path() {
      return path.get();
   }

   @Override
   public String toString() {
      return "StackFileConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
