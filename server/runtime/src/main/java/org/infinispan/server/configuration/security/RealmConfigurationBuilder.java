package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.RealmConfiguration.CACHE_LIFESPAN;
import static org.infinispan.server.configuration.security.RealmConfiguration.CACHE_MAX_SIZE;
import static org.infinispan.server.configuration.security.RealmConfiguration.DEFAULT_REALM;
import static org.infinispan.server.configuration.security.RealmConfiguration.EVIDENCE_DECODER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.server.EvidenceDecoder;

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

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RealmConfigurationBuilder defaultRealm(String defaultRealm) {
      this.attributes.attribute(DEFAULT_REALM).set(defaultRealm);
      return this;
   }

   public RealmConfigurationBuilder cacheMaxSize(int size) {
      this.attributes.attribute(CACHE_MAX_SIZE).set(size);
      return this;
   }

   public RealmConfigurationBuilder cacheLifespan(long lifespan) {
      this.attributes.attribute(CACHE_LIFESPAN).set(lifespan);
      return this;
   }

   public RealmConfigurationBuilder evidenceDecoder(EvidenceDecoder evidenceDecoder) {
      this.attributes.attribute(EVIDENCE_DECODER).set(evidenceDecoder);
      return this;
   }

   public AggregateRealmConfigurationBuilder aggregateConfiguration() {
      return addBuilder(Element.AGGREGATE_REALM, new AggregateRealmConfigurationBuilder());
   }

   public DistributedRealmConfigurationBuilder distributedConfiguration() {
      return addBuilder(Element.DISTRIBUTED_REALM, new DistributedRealmConfigurationBuilder());
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
      return addBuilder(Element.TRUSTSTORE_REALM, new TrustStoreRealmConfigurationBuilder());
   }

   public PropertiesRealmConfigurationBuilder propertiesConfiguration() {
      return addBuilder(Element.PROPERTIES_REALM, new PropertiesRealmConfigurationBuilder());
   }

   public ServerIdentitiesConfigurationBuilder serverIdentitiesConfiguration() {
      return serverIdentitiesConfiguration;
   }

   private <T extends RealmProviderBuilder> T addBuilder(Enum<?> type, T builder) {
      for(RealmProviderBuilder<?> b : builders) {
         if (b.getClass().equals(builder.getClass())) {
            throw Server.log.duplicateRealmType(type.toString(), attributes.attribute(RealmConfiguration.NAME).get());
         }
      }
      builders.add(builder);
      return builder;
   }

   @Override
   public void validate() {
      serverIdentitiesConfiguration.validate();
      Set<String> names = new HashSet<>();
      for(RealmProviderBuilder<?> builder : builders) {
         if (names.contains(builder.name())) {
            throw Server.log.duplicateRealm(builder.name());
         }
         builder.validate();
      }
   }

   @Override
   public RealmConfiguration create() {
      Collections.sort(builders);
      return new RealmConfiguration(
            attributes.protect(),
            serverIdentitiesConfiguration.create(),
            builders.stream().map(RealmProviderBuilder::create).collect(Collectors.toList())
      );
   }

   @Override
   public RealmConfigurationBuilder read(RealmConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      serverIdentitiesConfiguration.read(template.serverIdentitiesConfiguration(), combine);
      this.builders.clear();
      for(RealmProvider provider : template.realmProviders()) {
         RealmProviderBuilder builder = Util.getInstance(provider.getClass().getAnnotation(BuiltBy.class).value().asSubclass(RealmProviderBuilder.class));
         builder.read(provider, combine);
         builders.add(builder);
      }
      return this;
   }
}
