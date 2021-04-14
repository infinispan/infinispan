package org.infinispan.server.configuration.security;

/**
 * @since 10.0
 */
public class SecurityConfiguration {
   private final CredentialStoresConfiguration credentialStoresConfiguration;
   private final RealmsConfiguration realmsConfiguration;

   SecurityConfiguration(CredentialStoresConfiguration credentialStoresConfiguration, RealmsConfiguration realmsConfiguration) {
      this.credentialStoresConfiguration = credentialStoresConfiguration;
      this.realmsConfiguration = realmsConfiguration;
   }

   public CredentialStoresConfiguration credentialStores() {
      return credentialStoresConfiguration;
   }

   public RealmsConfiguration realms() {
      return realmsConfiguration;
   }
}
