package org.infinispan.server.configuration.endpoint;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.SocketBindingsConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;

public class EndpointsConfigurationBuilder implements Builder<EndpointsConfiguration> {

   private final AttributeSet attributes;
   private final ServerConfigurationBuilder server;

   private final Map<String, EndpointConfigurationBuilder> endpoints = new LinkedHashMap<>(2);
   private EndpointConfigurationBuilder current;

   public EndpointsConfigurationBuilder(ServerConfigurationBuilder server) {
      this.server = server;
      attributes = EndpointsConfiguration.attributeDefinitionSet();
   }

   public EndpointConfigurationBuilder addEndpoint(String socketBindingName) {
      if (endpoints.containsKey(socketBindingName)) {
         throw Server.log.endpointSocketBindingConflict(socketBindingName);
      }
      EndpointConfigurationBuilder builder = new EndpointConfigurationBuilder(server, socketBindingName);
      endpoints.put(socketBindingName, builder);
      this.current = builder;
      return builder;
   }

   public EndpointConfigurationBuilder current() {
      return current;
   }

   public Map<String, EndpointConfigurationBuilder> endpoints() {
      return endpoints;
   }

   @Override
   public EndpointsConfiguration create() {
      throw new UnsupportedOperationException();
   }

   public EndpointsConfiguration create(GlobalConfigurationBuilder builder, SocketBindingsConfiguration bindingsConfiguration, SecurityConfiguration securityConfiguration) {
      List<EndpointConfiguration> list = endpoints.values().stream()
            .map(e -> e.create(bindingsConfiguration, securityConfiguration)).collect(Collectors.toList());
      // When authz is enabled, ensure that at least one endpoint has authn, otherwise the server will be useless.
      // This can only be done after implicit settings have been applied
      boolean withAuthn = false;
      if (builder.security().authorization().isEnabled()) {
         for (EndpointConfiguration endpoint : list) {
            for (ProtocolServerConfiguration<?> connector : endpoint.connectors()) {
               if (connector instanceof HotRodServerConfiguration) {
                  if (((HotRodServerConfiguration) connector).authentication().enabled()) {
                     withAuthn = true;
                  }
               }
               if (connector instanceof RestServerConfiguration) {
                  if (((RestServerConfiguration) connector).authentication().enabled()) {
                     withAuthn = true;
                  }
               }
            }
         }
         if (!withAuthn) {
            throw Server.log.authorizationWithoutAuthentication();
         }
      }
      return new EndpointsConfiguration(attributes.protect(), list);
   }

   @Override
   public EndpointsConfigurationBuilder read(EndpointsConfiguration template) {
      this.attributes.read(template.attributes());
      endpoints.clear();
      return this;
   }

   @Override
   public void validate() {
      for (EndpointConfigurationBuilder endpoint : endpoints.values()) {
         endpoint.validate();
      }
   }
}
