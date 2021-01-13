package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class SecurityConfigurationBuilder implements Builder<SecurityConfiguration> {
   private final CredentialStoresConfigurationBuilder credentialStoresConfiguration = new CredentialStoresConfigurationBuilder();
   private final RealmsConfigurationBuilder realmsConfiguration = new RealmsConfigurationBuilder();
   private final TransportSecurityConfigurationBuilder transport = new TransportSecurityConfigurationBuilder();

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
      transport.validate();
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(credentialStoresConfiguration.create(), realmsConfiguration.create(), transport.create());
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template) {
      credentialStoresConfiguration.read(template.credentialStores());
      realmsConfiguration.read(template.realms());
      return this;
   }

   public TransportSecurityConfigurationBuilder transport() {
      return transport;
   }
}
