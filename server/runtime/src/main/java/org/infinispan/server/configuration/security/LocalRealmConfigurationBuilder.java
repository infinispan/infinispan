package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class LocalRealmConfigurationBuilder implements RealmProviderBuilder<LocalRealmConfiguration> {
   private final AttributeSet attributes;

   LocalRealmConfigurationBuilder() {
      this.attributes = LocalRealmConfiguration.attributeDefinitionSet();
   }

   public LocalRealmConfigurationBuilder name(String name) {
      attributes.attribute(LocalRealmConfiguration.NAME).set(name);
      return this;
   }

   @Override
   public LocalRealmConfiguration create() {
      return new LocalRealmConfiguration(attributes.protect());
   }

   @Override
   public LocalRealmConfigurationBuilder read(LocalRealmConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
