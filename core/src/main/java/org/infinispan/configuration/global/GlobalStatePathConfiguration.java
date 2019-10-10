package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.ParseUtils;

public class GlobalStatePathConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class)
         .initializer(() -> SecurityActions.getSystemProperty("user.dir"))
         .immutable().build();

   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).immutable().build();

   private final Attribute<String> path;
   private final Attribute<String> relativeTo;
   private final String elementName;
   private final ElementDefinition elementDefinition;
   private final String location;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, PATH, RELATIVE_TO);
   }

   private final AttributeSet attributes;

   public GlobalStatePathConfiguration(AttributeSet attributes, String elementName) {
      this.attributes = attributes.checkProtection();
      this.path = attributes.attribute(PATH);
      this.relativeTo = attributes.attribute(RELATIVE_TO);
      this.elementName = elementName;
      this.location = ParseUtils.resolvePath(path.get(), relativeTo.get());
      this.elementDefinition = new DefaultElementDefinition<>(elementName);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String getLocation() {
      return location;
   }

   public String path() {
      return path.get();
   }

   public String relativeTo() {
      return relativeTo.get();
   }

   String elementName() {
      return elementName;
   }
}
