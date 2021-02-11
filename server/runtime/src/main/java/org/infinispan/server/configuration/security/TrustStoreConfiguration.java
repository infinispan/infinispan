package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.PasswordSerializer;

/**
 * @since 12.1
 */
public class TrustStoreConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<char[]> PASSWORD = AttributeDefinition.builder("password", null, char[].class)
         .serializer(PasswordSerializer.INSTANCE).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder("provider", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, PASSWORD, PATH, RELATIVE_TO, PROVIDER);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.TRUSTSTORE.toString());

   private final AttributeSet attributes;

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   TrustStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }
}
