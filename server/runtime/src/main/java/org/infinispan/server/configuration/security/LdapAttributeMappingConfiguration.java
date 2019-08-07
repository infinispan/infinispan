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
public class LdapAttributeMappingConfiguration implements ConfigurationInfo {

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.ATTRIBUTE_MAPPING.toString());

   private List<ConfigurationInfo> elements = new ArrayList<>();
   private final List<LdapAttributeConfiguration> attributesConfiguration;

   LdapAttributeMappingConfiguration(List<LdapAttributeConfiguration> attributesConfiguration) {
      this.attributesConfiguration = attributesConfiguration;
      elements.addAll(attributesConfiguration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   List<LdapAttributeConfiguration> attributesConfiguration() {
      return attributesConfiguration;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

}
