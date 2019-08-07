package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

public class UserPropertiesConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> DIGEST_REALM_NAME = AttributeDefinition.builder("digestRealmName", null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();
   static final AttributeDefinition<Boolean> PLAIN_TEXT = AttributeDefinition.builder("plainText", null, Boolean.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.USER_PROPERTIES.toString());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(UserPropertiesConfiguration.class, DIGEST_REALM_NAME, PATH, PLAIN_TEXT, RELATIVE_TO);
   }

   private final AttributeSet attributes;

   UserPropertiesConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public String path() {
      return attributes.attribute(PATH).get();
   }

   public String relativeTo() {
      return attributes.attribute(RELATIVE_TO).get();
   }

   public boolean plainText() {
      return attributes.attribute(PLAIN_TEXT).get();
   }

   public String digestRealmName() {
      return attributes.attribute(DIGEST_REALM_NAME).get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public String toString() {
      return "UserPropertiesConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UserPropertiesConfiguration that = (UserPropertiesConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
