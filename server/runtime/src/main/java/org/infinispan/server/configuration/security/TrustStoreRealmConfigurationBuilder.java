package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.DistributedRealmConfiguration.NAME;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class TrustStoreRealmConfigurationBuilder implements RealmProviderBuilder<TrustStoreRealmConfiguration> {
   private final AttributeSet attributes;

   TrustStoreRealmConfigurationBuilder() {
      this.attributes = TrustStoreRealmConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public TrustStoreRealmConfigurationBuilder name(String name) {
      attributes.attribute(TrustStoreRealmConfiguration.NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public TrustStoreRealmConfiguration create() {
      return new TrustStoreRealmConfiguration(attributes.protect());
   }

   @Override
   public TrustStoreRealmConfigurationBuilder read(TrustStoreRealmConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return -1; // Must be the first
   }
}
