package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class TrustStoreRealmConfigurationBuilder implements RealmProviderBuilder<TrustStoreRealmConfiguration> {
   private final AttributeSet attributes;

   TrustStoreRealmConfigurationBuilder() {
      this.attributes = TrustStoreRealmConfiguration.attributeDefinitionSet();
   }

   public TrustStoreRealmConfigurationBuilder name(String name) {
      attributes.attribute(TrustStoreRealmConfiguration.NAME).set(name);
      return this;
   }

   @Override
   public TrustStoreRealmConfiguration create() {
      return new TrustStoreRealmConfiguration(attributes.protect());
   }

   @Override
   public TrustStoreRealmConfigurationBuilder read(TrustStoreRealmConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
