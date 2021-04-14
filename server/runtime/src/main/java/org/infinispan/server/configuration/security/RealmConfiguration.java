package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class RealmConfiguration extends ConfigurationElement<RealmConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<Integer> CACHE_MAX_SIZE = AttributeDefinition.builder(Attribute.CACHE_MAX_SIZE, 256).build();
   static final AttributeDefinition<Long> CACHE_LIFESPAN = AttributeDefinition.builder(Attribute.CACHE_LIFESPAN, -1l).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RealmConfiguration.class, NAME, CACHE_MAX_SIZE, CACHE_LIFESPAN);
   }

   private final FileSystemRealmConfiguration fileSystemConfiguration;
   private final LdapRealmConfiguration ldapConfiguration;
   private final LocalRealmConfiguration localConfiguration;
   private final TokenRealmConfiguration tokenConfiguration;
   private final TrustStoreRealmConfiguration trustStoreConfiguration;
   private final ServerIdentitiesConfiguration serverIdentitiesConfiguration;
   private final PropertiesRealmConfiguration propertiesRealmConfiguration;

   RealmConfiguration(AttributeSet attributes,
                      FileSystemRealmConfiguration fileSystemConfiguration,
                      LdapRealmConfiguration ldapConfiguration,
                      LocalRealmConfiguration localConfiguration,
                      TokenRealmConfiguration tokenConfiguration,
                      TrustStoreRealmConfiguration trustStoreConfiguration,
                      ServerIdentitiesConfiguration serverIdentitiesConfiguration,
                      PropertiesRealmConfiguration propertiesRealmConfiguration) {
      super(Element.SECURITY_REALM, attributes);
      this.fileSystemConfiguration = fileSystemConfiguration;
      this.ldapConfiguration = ldapConfiguration;
      this.localConfiguration = localConfiguration;
      this.tokenConfiguration = tokenConfiguration;
      this.trustStoreConfiguration = trustStoreConfiguration;
      this.serverIdentitiesConfiguration = serverIdentitiesConfiguration;
      this.propertiesRealmConfiguration = propertiesRealmConfiguration;
   }

   public FileSystemRealmConfiguration fileSystemConfiguration() {
      return fileSystemConfiguration;
   }

   public LdapRealmConfiguration ldapConfiguration() {
      return ldapConfiguration;
   }

   public LocalRealmConfiguration localConfiguration() {
      return localConfiguration;
   }

   public TokenRealmConfiguration tokenConfiguration() {
      return tokenConfiguration;
   }

   public TrustStoreRealmConfiguration trustStoreConfiguration() {
      return trustStoreConfiguration;
   }

   public ServerIdentitiesConfiguration serverIdentitiesConfiguration() {
      return serverIdentitiesConfiguration;
   }

   public PropertiesRealmConfiguration propertiesRealm() {
      return propertiesRealmConfiguration;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public String toString() {
      return "RealmConfiguration{" +
            "attributes=" + attributes +
            ", fileSystemConfiguration=" + fileSystemConfiguration +
            ", ldapConfiguration=" + ldapConfiguration +
            ", localConfiguration=" + localConfiguration +
            ", tokenConfiguration=" + tokenConfiguration +
            ", trustStoreConfiguration=" + trustStoreConfiguration +
            ", serverIdentitiesConfiguration=" + serverIdentitiesConfiguration +
            ", propertiesRealmConfiguration=" + propertiesRealmConfiguration +
            '}';
   }
}
