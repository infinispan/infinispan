package org.infinispan.server.configuration.endpoint;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.configuration.Attribute;

/**
 * Holds configuration related to endpoints
 * @author Tristan Tarrant
 * @since 12.0
 */
public class EndpointsConfiguration {
   static final AttributeDefinition<String> SOCKET_BINDING = AttributeDefinition.builder(Attribute.SOCKET_BINDING, null, String.class).build();
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder(Attribute.SECURITY_REALM, null, String.class).build();

   private final List<EndpointConfiguration> endpoints;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EndpointsConfiguration.class, SOCKET_BINDING, SECURITY_REALM);
   }

   private final AttributeSet attributes;

   EndpointsConfiguration(AttributeSet attributes, List<EndpointConfiguration> endpoints) {
      this.attributes = attributes.checkProtection();
      this.endpoints = endpoints;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public String socketBinding() {
      return attributes.attribute(SOCKET_BINDING).get();
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
