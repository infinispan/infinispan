package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class KeyStoreConfiguration extends ConfigurationElement<KeyStoreConfiguration> {
   static final AttributeDefinition<String> ALIAS = AttributeDefinition.builder(Attribute.ALIAS,null, String.class).build();
   static final AttributeDefinition<String> GENERATE_SELF_SIGNED_CERTIFICATE_HOST = AttributeDefinition.builder(Attribute.GENERATE_SELF_SIGNED_CERTIFICATE_HOST, null, String.class).build();
   static final AttributeDefinition<char[]> KEY_PASSWORD = AttributeDefinition.builder(Attribute.KEY_PASSWORD, null, char[].class)
         .serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<char[]> KEYSTORE_PASSWORD = AttributeDefinition.builder(Attribute.KEYSTORE_PASSWORD, null, char[].class)
         .serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder(Attribute.PROVIDER, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, ALIAS, GENERATE_SELF_SIGNED_CERTIFICATE_HOST, KEY_PASSWORD, KEYSTORE_PASSWORD, PATH, RELATIVE_TO, PROVIDER);
   }

   KeyStoreConfiguration(AttributeSet attributes) {
      super(Element.KEYSTORE, attributes);
   }
}
