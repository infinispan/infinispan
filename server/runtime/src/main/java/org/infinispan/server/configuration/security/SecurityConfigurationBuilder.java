package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.configuration.ServerConfigurationBuilder;

/**
 * @since 10.0
 */
public class SecurityConfigurationBuilder implements Builder<SecurityConfiguration> {
   private final ProvidersConfigurationBuilder providersConfiguration;
   private final CredentialStoresConfigurationBuilder credentialStoresConfiguration;
   private final RealmsConfigurationBuilder realmsConfiguration = new RealmsConfigurationBuilder();
   private final TransportSecurityConfigurationBuilder transport = new TransportSecurityConfigurationBuilder();
   private final ServerConfigurationBuilder builder;

   public SecurityConfigurationBuilder(ServerConfigurationBuilder builder) {
      this.builder = builder;
      this.providersConfiguration = new ProvidersConfigurationBuilder(builder);
      this.credentialStoresConfiguration = new CredentialStoresConfigurationBuilder(builder);
   }

   public CredentialStoresConfigurationBuilder credentialStores() {
      return credentialStoresConfiguration;
   }

   public ProvidersConfigurationBuilder providers() {
      return providersConfiguration;
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
      return new SecurityConfiguration(providersConfiguration.create(), credentialStoresConfiguration.create(), realmsConfiguration.create(), transport.create(), builder.properties());
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template) {
      providersConfiguration.read(template.providers());
      credentialStoresConfiguration.read(template.credentialStores());
      realmsConfiguration.read(template.realms());
      return this;
   }

   public TransportSecurityConfigurationBuilder transport() {
      return transport;
   }
}
