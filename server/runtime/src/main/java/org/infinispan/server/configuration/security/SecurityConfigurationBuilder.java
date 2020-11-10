package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class SecurityConfigurationBuilder implements Builder<SecurityConfiguration> {
   private final CredentialStoresConfigurationBuilder credentialStoresConfiguration = new CredentialStoresConfigurationBuilder();
   private final RealmsConfigurationBuilder realmsConfiguration = new RealmsConfigurationBuilder();

   public CredentialStoresConfigurationBuilder credentialStores() {
      return credentialStoresConfiguration;
   }

   public RealmsConfigurationBuilder realms() {
      return realmsConfiguration;
   }

   @Override
   public void validate() {
      credentialStoresConfiguration.validate();
      realmsConfiguration.validate();
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(credentialStoresConfiguration.create(), realmsConfiguration.create());
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template) {
      credentialStoresConfiguration.read(template.credentialStores());
      realmsConfiguration.read(template.realms());
      return this;
   }
}
