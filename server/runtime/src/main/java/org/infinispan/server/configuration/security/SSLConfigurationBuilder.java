package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class SSLConfigurationBuilder implements Builder<SSLConfiguration> {
   private final KeyStoreConfigurationBuilder keyStore;
   private final TrustStoreConfigurationBuilder trustStore;
   private final SSLEngineConfigurationBuilder engine;


   SSLConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.keyStore = new KeyStoreConfigurationBuilder(realmBuilder);
      this.trustStore = new TrustStoreConfigurationBuilder(realmBuilder);
      this.engine = new SSLEngineConfigurationBuilder(realmBuilder);
   }

   public KeyStoreConfigurationBuilder keyStore() {
      return keyStore;
   }

   public TrustStoreConfigurationBuilder trustStore() {
      return trustStore;
   }

   public SSLEngineConfigurationBuilder engine() {
      return engine;
   }

   @Override
   public SSLConfiguration create() {
      return new SSLConfiguration(keyStore.create(), trustStore.create(), engine.create());
   }

   @Override
   public SSLConfigurationBuilder read(SSLConfiguration template) {
      keyStore.read(template.keyStore());
      trustStore.read(template.trustStore());
      engine.read(template.engine());
      return this;
   }

}
