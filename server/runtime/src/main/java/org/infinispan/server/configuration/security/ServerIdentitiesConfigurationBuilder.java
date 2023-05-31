package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class ServerIdentitiesConfigurationBuilder implements Builder<ServerIdentitiesConfiguration> {
   SSLConfigurationBuilder sslConfigurationBuilder;
   private final List<KerberosSecurityFactoryConfigurationBuilder> kerberosConfigurations = new ArrayList<>();
   private final RealmConfigurationBuilder realmBuilder;

   ServerIdentitiesConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   public SSLConfigurationBuilder sslConfiguration() {
      if (sslConfigurationBuilder == null) {
         sslConfigurationBuilder = new SSLConfigurationBuilder(realmBuilder);
      }
      return sslConfigurationBuilder;
   }

   public KerberosSecurityFactoryConfigurationBuilder addKerberosConfiguration() {
      KerberosSecurityFactoryConfigurationBuilder kerberos = new KerberosSecurityFactoryConfigurationBuilder(realmBuilder);
      kerberosConfigurations.add(kerberos);
      return kerberos;
   }

   @Override
   public ServerIdentitiesConfiguration create() {
      SSLConfiguration sslConfiguration = this.sslConfigurationBuilder == null ? null : this.sslConfigurationBuilder.create();
      List<KerberosSecurityFactoryConfiguration> kerberosConfigurations = this.kerberosConfigurations.stream()
            .map(KerberosSecurityFactoryConfigurationBuilder::create).collect(Collectors.toList());
      return new ServerIdentitiesConfiguration(sslConfiguration, kerberosConfigurations);
   }

   @Override
   public ServerIdentitiesConfigurationBuilder read(ServerIdentitiesConfiguration template, Combine combine) {
      if (template.sslConfiguration() != null) {
         sslConfiguration().read(template.sslConfiguration(), combine);
      }
      kerberosConfigurations.clear();
      template.kerberosConfigurations().forEach(s -> addKerberosConfiguration().read(s, combine));
      return this;
   }
}
