package org.infinispan.server.core.configuration;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 10.0
 */
public class EncryptionConfiguration extends ConfigurationElement<EncryptionConfiguration> {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder(Attribute.SECURITY_REALM, null, String.class).build();
   static final AttributeDefinition<Boolean> REQUIRE_CLIENT_AUTH = AttributeDefinition.builder(Attribute.REQUIRE_SSL_CLIENT_AUTH, false, Boolean.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EncryptionConfiguration.class, REQUIRE_CLIENT_AUTH, SECURITY_REALM);
   }

   private final List<SniConfiguration> sniConfigurations;

   EncryptionConfiguration(AttributeSet attributes, List<SniConfiguration> sniConfigurations) {
      super("encryption", attributes, children(sniConfigurations));
      this.sniConfigurations = sniConfigurations;
   }

   public List<SniConfiguration> sniConfigurations() {
      return sniConfigurations;
   }

   public String realm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public boolean requireClientAuth() {
      return attributes.attribute(REQUIRE_CLIENT_AUTH).get();
   }
}
