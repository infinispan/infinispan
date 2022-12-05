package org.infinispan.configuration.global;

import java.nio.file.Paths;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class TemporaryGlobalStatePathConfiguration {

   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class)
         .initializer(() -> Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath().toString())
         .immutable().build();

   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).immutable().build();

   private final Attribute<String> path;
   private final Attribute<String> relativeTo;
   private final String location;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, PATH, RELATIVE_TO);
   }

   private final AttributeSet attributes;

   public TemporaryGlobalStatePathConfiguration(AttributeSet attributes, String location) {
      this.attributes = attributes.checkProtection();
      this.path = attributes.attribute(PATH);
      this.relativeTo = attributes.attribute(RELATIVE_TO);
      this.location = location;
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
}
