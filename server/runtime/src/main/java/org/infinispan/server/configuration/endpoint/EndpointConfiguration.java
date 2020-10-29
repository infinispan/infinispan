package org.infinispan.server.configuration.endpoint;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;

/**
 * @since 12.0
 */
public class EndpointConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> SOCKET_BINDING = AttributeDefinition.builder("socket-binding", null, String.class).build();
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).build();
   static final AttributeDefinition<Boolean> ADMIN = AttributeDefinition.builder("admin", true, Boolean.class).build();
   static final AttributeDefinition<Boolean> METRICS_AUTH = AttributeDefinition.builder("metrics-auth", true, Boolean.class).build();

   private final List<ProtocolServerConfiguration> connectors;
   private final SinglePortRouterConfiguration singlePort;


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EndpointConfiguration.class, SOCKET_BINDING, SECURITY_REALM, ADMIN, METRICS_AUTH);
   }

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.ENDPOINTS.toString());

   private final AttributeSet attributes;

   private final List<ConfigurationInfo> configs = new ArrayList<>();

   EndpointConfiguration(AttributeSet attributes,
                         List<ProtocolServerConfiguration> connectors,
                         SinglePortRouterConfiguration singlePort) {
      this.attributes = attributes.checkProtection();
      this.connectors = connectors;
      this.singlePort = singlePort;
      configs.addAll(connectors);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return configs;
   }

   public SinglePortRouterConfiguration singlePortRouter() {
      return singlePort;
   }

   public List<ProtocolServerConfiguration> connectors() {
      return connectors;
   }

   public boolean admin() {
      return attributes.attribute(ADMIN).get();
   }

   public boolean metricsAuth() {
      return attributes.attribute(METRICS_AUTH).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String socketBinding() {
      return attributes.attribute(SOCKET_BINDING).get();
   }
}
