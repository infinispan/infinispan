package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 12.1
 */
public class TrustStoreConfiguration extends ConfigurationElement<TrustStoreConfiguration> {
   static final AttributeDefinition<char[]> PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, char[].class).serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder(Attribute.PROVIDER, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, PASSWORD, PATH, RELATIVE_TO, PROVIDER);
   }

   TrustStoreConfiguration(AttributeSet attributes) {
      super(Element.TRUSTSTORE, attributes);
   }
}
