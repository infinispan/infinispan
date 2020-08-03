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
 * @since 10.0
 */
public class EndpointConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> SOCKET_BINDING = AttributeDefinition.builder("socket-binding", null, String.class).build();
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).build();
   static final AttributeDefinition<Boolean> IMPLICIT_CONNECTOR_SECURITY = AttributeDefinition.builder("implicit-connector-security", false, Boolean.class).build();

   private final List<ProtocolServerConfiguration> connectors;
   private final SinglePortRouterConfiguration singlePort;


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EndpointConfiguration.class, SOCKET_BINDING, SECURITY_REALM, IMPLICIT_CONNECTOR_SECURITY);
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

   public boolean implicitConnectorSecurity() {
      return attributes.attribute(IMPLICIT_CONNECTOR_SECURITY).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
