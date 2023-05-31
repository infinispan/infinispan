package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.configuration.ServerConfigurationBuilder;

/**
 * @since 10.0
 */
public class SecurityConfigurationBuilder implements Builder<SecurityConfiguration> {
   private final CredentialStoresConfigurationBuilder credentialStoresConfiguration;
   private final RealmsConfigurationBuilder realmsConfiguration = new RealmsConfigurationBuilder();
   private final TransportSecurityConfigurationBuilder transport = new TransportSecurityConfigurationBuilder();
   private final ServerConfigurationBuilder builder;

   public SecurityConfigurationBuilder(ServerConfigurationBuilder builder) {
      this.builder = builder;
      this.credentialStoresConfiguration = new CredentialStoresConfigurationBuilder(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

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
      return new SecurityConfiguration(credentialStoresConfiguration.create(), realmsConfiguration.create(), transport.create(), builder.properties());
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template, Combine combine) {
      credentialStoresConfiguration.read(template.credentialStores(), combine);
      realmsConfiguration.read(template.realms(), combine);
      return this;
   }

   public TransportSecurityConfigurationBuilder transport() {
      return transport;
   }
}
