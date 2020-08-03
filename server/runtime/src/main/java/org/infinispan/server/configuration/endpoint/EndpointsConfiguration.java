package org.infinispan.server.configuration.endpoint;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * Holds configuration related to endpoints
 * @author Tristan Tarrant
 * @since 12.0
 */
public class EndpointsConfiguration implements ConfigurationInfo {

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.ENDPOINTS.toString());

   private final List<EndpointConfiguration> endpoints;
   private final List<ConfigurationInfo> configs = new ArrayList<>();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EndpointsConfiguration.class);
   }

   private final AttributeSet attributes;

   EndpointsConfiguration(AttributeSet attributes, List<EndpointConfiguration> endpoints) {
      this.attributes = attributes.checkProtection();
      this.endpoints = endpoints;
      this.configs.addAll(endpoints);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return configs;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public List<EndpointConfiguration> endpoints() {
      return endpoints;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EndpointsConfiguration that = (EndpointsConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "EndpointConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
