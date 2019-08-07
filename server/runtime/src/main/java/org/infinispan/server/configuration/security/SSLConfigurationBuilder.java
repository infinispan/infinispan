package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class SSLConfigurationBuilder implements Builder<SSLConfiguration> {
   private final KeyStoreConfigurationBuilder keyStore;
   private final SSLEngineConfigurationBuilder engine;

   SSLConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.keyStore = new KeyStoreConfigurationBuilder(realmBuilder);
      this.engine = new SSLEngineConfigurationBuilder(realmBuilder);
   }

   public KeyStoreConfigurationBuilder keyStore() {
      return keyStore;
   }

   public SSLEngineConfigurationBuilder engine() {
      return engine;
   }

   @Override
   public void validate() {
   }

   @Override
   public SSLConfiguration create() {
      return new SSLConfiguration(keyStore.create(), engine.create());
   }

   @Override
   public SSLConfigurationBuilder read(SSLConfiguration template) {
      keyStore.read(template.keyStore());
      engine.read(template.engine());
      return this;
   }
}
