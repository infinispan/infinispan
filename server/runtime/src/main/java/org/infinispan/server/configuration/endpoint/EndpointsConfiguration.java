package org.infinispan.server.configuration.endpoint;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Holds configuration related to endpoints
 * @author Tristan Tarrant
 * @since 12.0
 */
public class EndpointsConfiguration {

   private final List<EndpointConfiguration> endpoints;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EndpointsConfiguration.class);
   }

   private final AttributeSet attributes;

   EndpointsConfiguration(AttributeSet attributes, List<EndpointConfiguration> endpoints) {
      this.attributes = attributes.checkProtection();
      this.endpoints = endpoints;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public List<EndpointConfiguration> endpoints() {
      return endpoints;
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
