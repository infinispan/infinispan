package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @since 10.0
 */
public class ServerIdentitiesConfiguration implements ConfigurationInfo {

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERVER_IDENTITIES.toString());

   private final List<SSLConfiguration> sslConfigurations;
   private final List<KerberosSecurityFactoryConfiguration> kerberosConfigurations;
   private final List<ConfigurationInfo> elements = new ArrayList<>();

   ServerIdentitiesConfiguration(List<SSLConfiguration> sslConfigurations, List<KerberosSecurityFactoryConfiguration> kerberosConfigurations) {
      this.sslConfigurations = sslConfigurations;
      this.elements.addAll(sslConfigurations);
      this.kerberosConfigurations = kerberosConfigurations;
      this.elements.addAll(kerberosConfigurations);
   }

   public List<SSLConfiguration> sslConfigurations() {
      return sslConfigurations;
   }

   public List<KerberosSecurityFactoryConfiguration> kerberosConfigurations() {
      return kerberosConfigurations;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
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
