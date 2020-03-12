package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

public class TemporaryGlobalStatePathConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class)
         .initializer(() -> SecurityActions.getSystemProperty("java.io.tmpdir"))
         .immutable().build();

   public static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).immutable().build();

   private final Attribute<String> path;
   private final Attribute<String> relativeTo;
   private final String location;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, PATH, RELATIVE_TO);
   }

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.TEMPORARY_LOCATION.getLocalName());

   private final AttributeSet attributes;

   public TemporaryGlobalStatePathConfiguration(AttributeSet attributes, String location) {
      this.attributes = attributes.checkProtection();
      this.path = attributes.attribute(PATH);
      this.relativeTo = attributes.attribute(RELATIVE_TO);
      this.location = location;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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
}
