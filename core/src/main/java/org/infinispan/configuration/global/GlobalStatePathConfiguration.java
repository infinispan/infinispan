package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.ParseUtils;

@BuiltBy(GlobalStateConfigurationBuilder.class)
public class GlobalStatePathConfiguration extends ConfigurationElement<GlobalStatePathConfiguration> {

   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class)
         .initializer(() -> System.getProperty("user.dir"))
         .immutable().build();

   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, null, String.class).immutable().build();

   private final String location;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, PATH, RELATIVE_TO);
   }

   public GlobalStatePathConfiguration(AttributeSet attributes, String elementName) {
      super(elementName, attributes);
      this.location = ParseUtils.resolvePath(attributes.attribute(PATH).get(), attributes.attribute(RELATIVE_TO).get());
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
