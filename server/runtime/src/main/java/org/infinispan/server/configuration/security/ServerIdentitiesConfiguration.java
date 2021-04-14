package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.server.Server;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @since 10.0
 */
public class ServerIdentitiesConfiguration {

   private final SSLConfiguration sslConfiguration;
   private final List<KerberosSecurityFactoryConfiguration> kerberosConfigurations;

   ServerIdentitiesConfiguration(SSLConfiguration sslConfiguration, List<KerberosSecurityFactoryConfiguration> kerberosConfigurations) {
      this.sslConfiguration = sslConfiguration;
      this.kerberosConfigurations = kerberosConfigurations;
   }

   public SSLConfiguration sslConfiguration() {
      return sslConfiguration;
   }

   public List<KerberosSecurityFactoryConfiguration> kerberosConfigurations() {
      return kerberosConfigurations;
   }

   public CredentialSource getCredentialSource(String serverPrincipal) {
      for (KerberosSecurityFactoryConfiguration configuration : kerberosConfigurations) {
         if (configuration.getPrincipal().equals(serverPrincipal)) {
            return configuration.getCredentialSource();
         }
      }
      throw Server.log.unknownServerIdentity(serverPrincipal);
   }
}
