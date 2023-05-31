package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

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

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
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
   public SSLConfigurationBuilder read(SSLConfiguration template, Combine combine) {
      keyStore.read(template.keyStore(), combine);
      trustStore.read(template.trustStore(), combine);
      engine.read(template.engine(), combine);
      return this;
   }

}
