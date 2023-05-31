package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.AggregateRealmConfiguration.AUTHN_REALM;
import static org.infinispan.server.configuration.security.AggregateRealmConfiguration.AUTHZ_REALMS;
import static org.infinispan.server.configuration.security.AggregateRealmConfiguration.NAME_REWRITER;
import static org.infinispan.server.configuration.security.DistributedRealmConfiguration.NAME;

import java.util.Arrays;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.wildfly.security.auth.server.NameRewriter;

/**
 * @since 15.0
 **/
public class AggregateRealmConfigurationBuilder implements RealmProviderBuilder<AggregateRealmConfiguration> {

   private final AttributeSet attributes;

   public AggregateRealmConfigurationBuilder() {
      this.attributes = AggregateRealmConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public AggregateRealmConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   public AggregateRealmConfigurationBuilder authnRealm(String realm) {
      attributes.attribute(AUTHN_REALM).set(realm);
      return this;
   }

   public AggregateRealmConfigurationBuilder nameRewriter(NameRewriter nameRewriter) {
      attributes.attribute(NAME_REWRITER).set(nameRewriter);
      return this;
   }

   public AggregateRealmConfigurationBuilder authzRealms(String[] realms) {
      attributes.attribute(AUTHZ_REALMS).set(Arrays.asList(realms));
      return this;
   }

   @Override
   public AggregateRealmConfiguration create() {
      return new AggregateRealmConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(AggregateRealmConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return 1; // Must be the last
   }
}
