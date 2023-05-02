package org.infinispan.server.configuration.security;

import java.security.KeyStore;
import java.security.Provider;
import java.util.EnumSet;
import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.KeyStoreBackedSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @since 10.0
 */
@BuiltBy(TrustStoreRealmConfigurationBuilder.class)
public class TrustStoreRealmConfiguration extends ConfigurationElement<TrustStoreRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "trust", String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreRealmConfiguration.class, NAME);
   }

   TrustStoreRealmConfiguration(AttributeSet attributes) {
      super(Element.TRUSTSTORE_REALM, attributes);
   }

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      Provider[] providers = SslContextFactory.discoverSecurityProviders(Thread.currentThread().getContextClassLoader());
      KeyStore keyStore = realm.serverIdentitiesConfiguration().sslConfiguration().trustStore().trustStore(providers, properties);
      return new KeyStoreBackedSecurityRealm(keyStore);
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public void applyFeatures(EnumSet<ServerSecurityRealm.Feature> features) {
      features.add(ServerSecurityRealm.Feature.TRUST);
   }
}
