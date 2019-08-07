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
public class ServerIdentitiesConfiguration implements ConfigurationInfo {

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERVER_IDENTITIES.toString());

   private final List<SSLConfiguration> sslConfigurations;
   private final List<ConfigurationInfo> elements = new ArrayList<>();

   ServerIdentitiesConfiguration(List<SSLConfiguration> sslConfigurations) {
      this.sslConfigurations = sslConfigurations;
      this.elements.addAll(sslConfigurations);
   }

   List<SSLConfiguration> sslConfigurations() {
      return sslConfigurations;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }
}
