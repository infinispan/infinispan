package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 10.0
 */
public class SniConfiguration extends ConfigurationElement<SniConfiguration> {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).build();
   static final AttributeDefinition<String> HOST_NAME = AttributeDefinition.builder("host-name", null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SniConfiguration.class, HOST_NAME, SECURITY_REALM);
   }

   SniConfiguration(AttributeSet attributes) {
      super("sni", true, attributes);
   }

   public String realm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public String host() {
      return attributes.attribute(HOST_NAME).get();
   }
}
