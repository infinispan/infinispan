package org.infinispan.server.configuration.security;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class RealmConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<Integer> CACHE_MAX_SIZE = AttributeDefinition.builder("cacheMaxSize", 256).build();
   static final AttributeDefinition<Long> CACHE_LIFESPAN = AttributeDefinition.builder("lifespan", -1l).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RealmConfiguration.class, NAME, CACHE_MAX_SIZE, CACHE_LIFESPAN);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SECURITY_REALM.toString());

   private final AttributeSet attributes;
   private final FileSystemRealmConfiguration fileSystemConfiguration;
   private final LdapRealmConfiguration ldapConfiguration;
   private final LocalRealmConfiguration localConfiguration;
   private final TokenRealmConfiguration tokenConfiguration;
   private final TrustStoreRealmConfiguration trustStoreConfiguration;
   private final ServerIdentitiesConfiguration serverIdentitiesConfiguration;
   private final PropertiesRealmConfiguration propertiesRealmConfiguration;
   private final List<ConfigurationInfo> elements;

   RealmConfiguration(AttributeSet attributes,
                      FileSystemRealmConfiguration fileSystemConfiguration,
                      LdapRealmConfiguration ldapConfiguration,
                      LocalRealmConfiguration localConfiguration,
                      TokenRealmConfiguration tokenConfiguration,
                      TrustStoreRealmConfiguration trustStoreConfiguration,
                      ServerIdentitiesConfiguration serverIdentitiesConfiguration,
                      PropertiesRealmConfiguration propertiesRealmConfiguration) {
      this.attributes = attributes.checkProtection();
      this.fileSystemConfiguration = fileSystemConfiguration;
      this.ldapConfiguration = ldapConfiguration;
      this.localConfiguration = localConfiguration;
      this.tokenConfiguration = tokenConfiguration;
      this.trustStoreConfiguration = trustStoreConfiguration;
      this.serverIdentitiesConfiguration = serverIdentitiesConfiguration;
      this.propertiesRealmConfiguration = propertiesRealmConfiguration;
      this.elements = Arrays.asList(fileSystemConfiguration, ldapConfiguration, localConfiguration,
            tokenConfiguration, trustStoreConfiguration, serverIdentitiesConfiguration,
            propertiesRealmConfiguration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
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

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RealmConfiguration that = (RealmConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
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
