package org.infinispan.server.configuration.security;

import java.util.EnumSet;
import java.util.Properties;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.ssl.SSLContextBuilder;

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

   SSLContextBuilder build(Properties properties, EnumSet<ServerSecurityRealm.Feature> features) {
      SSLContextBuilder builder = new SSLContextBuilder().setWrap(false).setProviderName(SslContextFactory.getSslProvider());
      keyStore.build(builder, properties, features);
      trustStore.build(builder, properties);
      engine.build(builder);
      return builder;
   }
}
