package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class SecurityConfiguration implements ConfigurationInfo {
   private static final ElementDefinition<SecurityConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(Element.SECURITY.toString());

   private final List<ConfigurationInfo> elements;
   private final CredentialStoresConfiguration credentialStoresConfiguration;
   private final RealmsConfiguration realmsConfiguration;

   SecurityConfiguration(CredentialStoresConfiguration credentialStoresConfiguration, RealmsConfiguration realmsConfiguration) {
      this.credentialStoresConfiguration = credentialStoresConfiguration;
      this.realmsConfiguration = realmsConfiguration;
      elements = new ArrayList<>();
      elements.add(credentialStoresConfiguration);
      elements.add(realmsConfiguration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   @Override
   public ElementDefinition<SecurityConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public CredentialStoresConfiguration credentialStores() {
      return credentialStoresConfiguration;
   }

   public RealmsConfiguration realms() {
      return realmsConfiguration;
   }
}
