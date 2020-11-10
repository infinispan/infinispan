package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.PasswordSerializer;

/**
 * @since 10.0
 */
public class KeyStoreConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> ALIAS = AttributeDefinition.builder("alias", null, String.class).build();
   static final AttributeDefinition<String> GENERATE_SELF_SIGNED_CERTIFICATE_HOST = AttributeDefinition.builder("generateSelfSignedCertificateHost", null, String.class).build();
   static final AttributeDefinition<char[]> KEY_PASSWORD = AttributeDefinition.builder("keyPassword", null, char[].class)
         .serializer(PasswordSerializer.INSTANCE).build();
   static final AttributeDefinition<char[]> KEYSTORE_PASSWORD = AttributeDefinition.builder("keystorePassword", null, char[].class)
         .serializer(PasswordSerializer.INSTANCE).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder("provider", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, ALIAS, GENERATE_SELF_SIGNED_CERTIFICATE_HOST, KEY_PASSWORD, KEYSTORE_PASSWORD, PATH, RELATIVE_TO, PROVIDER);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.KEYSTORE.toString());

   private final AttributeSet attributes;

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   KeyStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }
}
