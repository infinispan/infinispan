package org.infinispan.server.configuration.security;

/**
 * @since 10.0
 */
public class SSLConfiguration {

   private final KeyStoreConfiguration keyStore;
   private final TrustStoreConfiguration trustStore;
   private final SSLEngineConfiguration engine;

   SSLConfiguration(KeyStoreConfiguration keyStore, TrustStoreConfiguration trustStore, SSLEngineConfiguration engine) {
      this.keyStore = keyStore;
      this.trustStore = trustStore;
      this.engine = engine;
   }

   public KeyStoreConfiguration keyStore() {
      return keyStore;
   }

   public TrustStoreConfiguration trustStore() {
      return trustStore;
   }

   public SSLEngineConfiguration engine() {
      return engine;
   }
}
