package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

public class RemoteServerConfiguration extends ConfigurationElement<RemoteServerConfiguration> {

   static final AttributeDefinition<String> HOST = AttributeDefinition.builder(Attribute.HOST, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder(Attribute.PORT, 11222).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteServerConfiguration.class, HOST, PORT);
   }

   RemoteServerConfiguration(AttributeSet attributes) {
      super(Element.REMOTE_SERVER, attributes);
   }

   public String host() {
      return attributes.attribute(HOST).get();
   }

   public int port() {
      return attributes.attribute(PORT).get();
   }
}
