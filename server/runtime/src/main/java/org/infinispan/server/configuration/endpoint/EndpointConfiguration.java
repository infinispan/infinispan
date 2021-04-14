package org.infinispan.server.configuration.endpoint;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;

/**
 * @since 12.0
 */
public class EndpointConfiguration extends ConfigurationElement<EndpointConfiguration> {
   static final AttributeDefinition<String> SOCKET_BINDING = AttributeDefinition.builder(Attribute.SOCKET_BINDING, null, String.class).build();
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder(Attribute.SECURITY_REALM, null, String.class).build();
   static final AttributeDefinition<Boolean> ADMIN = AttributeDefinition.builder(Attribute.ADMIN, true, Boolean.class).build();
   static final AttributeDefinition<Boolean> METRICS_AUTH = AttributeDefinition.builder(Attribute.METRICS_AUTH, true, Boolean.class).build();
   private final List<ProtocolServerConfiguration> connectors;
   private final SinglePortRouterConfiguration singlePort;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(EndpointConfiguration.class, SOCKET_BINDING, SECURITY_REALM, ADMIN, METRICS_AUTH);
   }

   EndpointConfiguration(AttributeSet attributes,
                         List<ProtocolServerConfiguration> connectors,
                         SinglePortRouterConfiguration singlePort) {
      super(Element.ENDPOINTS, attributes);
      this.connectors = connectors;
      this.singlePort = singlePort;
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

   public String socketBinding() {
      return attributes.attribute(SOCKET_BINDING).get();
   }
}
