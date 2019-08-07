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
public class RealmsConfiguration implements ConfigurationInfo {
   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SECURITY_REALMS.toString());

   private final List<ConfigurationInfo> elements;
   private final List<RealmConfiguration> realms;

   RealmsConfiguration(List<RealmConfiguration> realms) {
      this.realms = realms;
      elements = new ArrayList<>(realms);
   }

   public List<RealmConfiguration> realms() {
      return realms;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }
}
