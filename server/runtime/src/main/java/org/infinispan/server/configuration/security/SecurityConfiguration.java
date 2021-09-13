package org.infinispan.server.configuration.security;

import java.util.Properties;

/**
 * @since 10.0
 */
public class SecurityConfiguration {
   private final CredentialStoresConfiguration credentialStoresConfiguration;
   private final RealmsConfiguration realmsConfiguration;
   private final TransportSecurityConfiguration transportConfiguration;

   SecurityConfiguration(CredentialStoresConfiguration credentialStoresConfiguration, RealmsConfiguration realmsConfiguration, TransportSecurityConfiguration transportConfiguration, Properties properties) {
      this.credentialStoresConfiguration = credentialStoresConfiguration;
      this.realmsConfiguration = realmsConfiguration;
      this.transportConfiguration = transportConfiguration;
      realmsConfiguration.init(this, properties);
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
