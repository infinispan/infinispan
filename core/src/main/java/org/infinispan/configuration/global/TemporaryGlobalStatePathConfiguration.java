package org.infinispan.configuration.global;

import java.nio.file.Paths;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

public class TemporaryGlobalStatePathConfiguration extends ConfigurationElement<TemporaryGlobalStatePathConfiguration> {

   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class)
         .initializer(() -> Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath().toString())
         .immutable().build();

   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, null, String.class).immutable().build();

   private final String location;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, PATH, RELATIVE_TO);
   }

   public TemporaryGlobalStatePathConfiguration(AttributeSet attributes, String location) {
      super(Element.TEMPORARY_LOCATION, attributes);
      this.location = location;
   }

   public String getLocation() {
      return location;
   }

   public String path() {
      return attributes.attribute(PATH).get();
   }

   public String relativeTo() {
      return attributes.attribute(RELATIVE_TO).get();
   }
}
