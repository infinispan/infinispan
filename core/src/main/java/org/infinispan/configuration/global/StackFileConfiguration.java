package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/*
 * @since 10.0
 */
public class StackFileConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StackFileConfiguration.class, NAME, PATH);
   }

   private final Attribute<String> name;
   private final Attribute<String> path;
   private final AttributeSet attributes;

   StackFileConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.path = attributes.attribute(PATH);
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
