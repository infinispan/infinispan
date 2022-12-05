package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;

public class GlobalStatePathConfiguration {

   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class)
         .initializer(() -> System.getProperty("user.dir"))
         .immutable().build();

   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).immutable().build();

   private final Attribute<String> path;
   private final Attribute<String> relativeTo;
   private final String elementName;
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
   }

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

   public boolean isModified() {
      return path.isModified() || relativeTo.isModified();
   }
}
