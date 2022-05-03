package org.infinispan.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * ServerConfiguration.
 *
 * @since 14.0
 */
public class ServerConfiguration extends ConfigurationElement<ServerConfiguration> {
   static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", "127.0.0.1", String.class).build();
   public static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", 11222, Integer.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ServerConfiguration.class, HOST, PORT);
   }

   ServerConfiguration(AttributeSet attributes) {
      super("server", attributes);
   }

   public String host() {
      return attributes.attribute(HOST).get();
   }

   public int port() {
      return attributes.attribute(PORT).get();
   }
}
