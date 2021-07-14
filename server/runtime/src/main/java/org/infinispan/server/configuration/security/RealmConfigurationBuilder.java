package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.RealmConfiguration.CACHE_LIFESPAN;
import static org.infinispan.server.configuration.security.RealmConfiguration.CACHE_MAX_SIZE;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class RealmConfigurationBuilder implements Builder<RealmConfiguration> {
   private final AttributeSet attributes;
   private final ServerIdentitiesConfigurationBuilder serverIdentitiesConfiguration = new ServerIdentitiesConfigurationBuilder(this);
   private final List<RealmProviderBuilder<?>> builders = new ArrayList<>();

   RealmConfigurationBuilder(String name) {
      this.attributes = RealmConfiguration.attributeDefinitionSet();
      attributes.attribute(RealmConfiguration.NAME).set(name);
   }

   public RealmConfigurationBuilder cacheMaxSize(int size) {
      this.attributes.attribute(CACHE_MAX_SIZE).set(size);
      return this;
   }

   public RealmConfigurationBuilder cacheLifespan(long lifespan) {
      this.attributes.attribute(CACHE_LIFESPAN).set(lifespan);
      return this;
   }

   public FileSystemRealmConfigurationBuilder fileSystemConfiguration() {
      return addBuilder(Element.FILESYSTEM_REALM, new FileSystemRealmConfigurationBuilder());
   }

   public LdapRealmConfigurationBuilder ldapConfiguration() {
      return addBuilder(Element.LDAP_REALM, new LdapRealmConfigurationBuilder());
   }

   public LocalRealmConfigurationBuilder localConfiguration() {
      return addBuilder(Element.LOCAL_REALM, new LocalRealmConfigurationBuilder());
   }

   public TokenRealmConfigurationBuilder tokenConfiguration() {
      return addBuilder(Element.TOKEN_REALM, new TokenRealmConfigurationBuilder());
   }

   public TrustStoreRealmConfigurationBuilder trustStoreConfiguration() {
      return addBuilder(Element.TRUSTSTORE_REALM, new TrustStoreRealmConfigurationBuilder(), 0);
   }

   public PropertiesRealmConfigurationBuilder propertiesConfiguration() {
      return addBuilder(Element.PROPERTIES_REALM, new PropertiesRealmConfigurationBuilder());
   }

   public ServerIdentitiesConfigurationBuilder serverIdentitiesConfiguration() {
      return serverIdentitiesConfiguration;
   }

   private <T extends RealmProviderBuilder> T addBuilder(Enum<?> type, T builder) {
      return addBuilder(type, builder, builders.size());
   }

   private <T extends RealmProviderBuilder> T addBuilder(Enum<?> type, T builder, int index) {
      for(RealmProviderBuilder<?> b : builders) {
         if (b.getClass().equals(builder.getClass())) {
            throw Server.log.duplicateRealmType(type.toString(), attributes.attribute(RealmConfiguration.NAME).get());
         }
      }
      builders.add(index, builder);
      return builder;
   }

   @Override
   public void validate() {
      serverIdentitiesConfiguration.validate();
      builders.forEach(Builder::validate);
   }

   @Override
   public RealmConfiguration create() {
      return new RealmConfiguration(
            attributes.protect(),
            serverIdentitiesConfiguration.create(),
            builders.stream().map(RealmProviderBuilder::create).collect(Collectors.toList())
      );
   }

   @Override
   public RealmConfigurationBuilder read(RealmConfiguration template) {
      this.attributes.read(template.attributes());
      serverIdentitiesConfiguration.read(template.serverIdentitiesConfiguration());
      this.builders.clear();
      for(RealmProvider provider : template.realmProviders()) {
         RealmProviderBuilder builder = Util.getInstance(provider.getClass().getAnnotation(BuiltBy.class).value().asSubclass(RealmProviderBuilder.class));
         builder.read(provider);
         builders.add(builder);
      }
      return this;
   }
}
