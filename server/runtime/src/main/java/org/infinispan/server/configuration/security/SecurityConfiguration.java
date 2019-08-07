package org.infinispan.server.configuration.security;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class SecurityConfiguration implements ConfigurationInfo {
   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SECURITY.toString());

   private final List<ConfigurationInfo> elements;
   private final RealmsConfiguration realmsConfiguration;

   SecurityConfiguration(RealmsConfiguration realmsConfiguration) {
      this.realmsConfiguration = realmsConfiguration;
      elements = Collections.singletonList(realmsConfiguration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public RealmsConfiguration realms() {
      return realmsConfiguration;
   }
}
