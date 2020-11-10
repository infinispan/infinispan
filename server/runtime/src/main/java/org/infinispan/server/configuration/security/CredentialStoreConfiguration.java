package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.PasswordSerializer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoreConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();
   static final AttributeDefinition<String> TYPE = AttributeDefinition.builder("type", "pkcs12", String.class).build();
   static final AttributeDefinition<String> CREDENTIAL = AttributeDefinition.builder("credential", null, String.class).serializer(PasswordSerializer.INSTANCE).build();

   private final AttributeSet attributes;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CredentialStoreConfiguration.class, NAME, PATH, RELATIVE_TO, TYPE, CREDENTIAL);
   }

   private static final ElementDefinition<CredentialStoreConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(Element.CREDENTIAL_STORE.toString());

   CredentialStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition<CredentialStoreConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }
}
