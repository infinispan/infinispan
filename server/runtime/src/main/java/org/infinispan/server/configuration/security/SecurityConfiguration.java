package org.infinispan.server.configuration.security;

import java.util.Properties;

/**
 * @since 10.0
 */
public class SecurityConfiguration {
   private final CredentialStoresConfiguration credentialStoresConfiguration;
   private final RealmsConfiguration realmsConfiguration;
   private final TransportSecurityConfiguration transportConfiguration;
   private final ProvidersConfiguration providersConfiguration;

   SecurityConfiguration(ProvidersConfiguration providersConfiguration, CredentialStoresConfiguration credentialStoresConfiguration, RealmsConfiguration realmsConfiguration, TransportSecurityConfiguration transportConfiguration, Properties properties) {
      this.providersConfiguration = providersConfiguration;
      this.credentialStoresConfiguration = credentialStoresConfiguration;
      this.realmsConfiguration = realmsConfiguration;
      this.transportConfiguration = transportConfiguration;
      realmsConfiguration.init(this, properties);
   }

   public ProvidersConfiguration providers() {
      return providersConfiguration;
   }

   public CredentialStoresConfiguration credentialStores() {
      return credentialStoresConfiguration;
   }

   public RealmsConfiguration realms() {
      return realmsConfiguration;
   }

   public TransportSecurityConfiguration transport() {
      return transportConfiguration;
   }
}
