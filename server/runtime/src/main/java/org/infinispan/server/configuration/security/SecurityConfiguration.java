package org.infinispan.server.configuration.security;

import java.util.Properties;

/**
 * @since 10.0
 */
public class SecurityConfiguration {
   private final CredentialStoresConfiguration credentialStoresConfiguration;
   private final RealmsConfiguration realmsConfiguration;

   SecurityConfiguration(CredentialStoresConfiguration credentialStoresConfiguration, RealmsConfiguration realmsConfiguration, Properties properties) {
      this.credentialStoresConfiguration = credentialStoresConfiguration;
      this.realmsConfiguration = realmsConfiguration;
      realmsConfiguration.init(this, properties);
   }

   public CredentialStoresConfiguration credentialStores() {
      return credentialStoresConfiguration;
   }

   public RealmsConfiguration realms() {
      return realmsConfiguration;
   }
}
