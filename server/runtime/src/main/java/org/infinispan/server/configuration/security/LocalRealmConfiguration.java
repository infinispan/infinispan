package org.infinispan.server.configuration.security;

import java.util.EnumSet;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @since 10.0
 */
public class LocalRealmConfiguration extends ConfigurationElement<LocalRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "local", String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LocalRealmConfiguration.class, NAME);
   }

   LocalRealmConfiguration(AttributeSet attributes) {
      super(Element.LOCAL_REALM, attributes);
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      return null;
   }

   @Override
   public void applyFeatures(EnumSet<ServerSecurityRealm.Feature> features) {
      // Nothing to do
   }
}
