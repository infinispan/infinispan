package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class TrustStoreRealmConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<char[]> KEYSTORE_PASSWORD = AttributeDefinition.builder("keystorePassword", null, char[].class)
         .serializer(PasswordSerializer.INSTANCE)
         .build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder("provider", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreRealmConfiguration.class, NAME, KEYSTORE_PASSWORD, PATH, PROVIDER, RELATIVE_TO);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.TRUSTSTORE_REALM.toString());

   private final AttributeSet attributes;

   TrustStoreRealmConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
