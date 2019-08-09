package org.infinispan.server.configuration.endpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

public class EndpointsConfigurationBuilder implements Builder<EndpointsConfiguration> {
   private final AttributeSet attributes;
   private final ServerConfigurationBuilder serverConfigurationBuilder;
   private final List<ProtocolServerConfigurationBuilder<?, ?>> connectors = new ArrayList<>(2);
   private final SinglePortServerConfigurationBuilder singlePort = new SinglePortServerConfigurationBuilder();

   public EndpointsConfigurationBuilder(ServerConfigurationBuilder serverConfigurationBuilder) {
      this.serverConfigurationBuilder = serverConfigurationBuilder;
      this.attributes = EndpointsConfiguration.attributeDefinitionSet();
   }

   public EndpointsConfigurationBuilder socketBinding(String name) {
      attributes.attribute(EndpointsConfiguration.SOCKET_BINDING).set(name);
      serverConfigurationBuilder.applySocketBinding(name, serverConfigurationBuilder.endpoint());
      return this;
   }

   public EndpointsConfigurationBuilder securityRealm(String name) {
      attributes.attribute(EndpointsConfiguration.SECURITY_REALM).set(name);
      serverConfigurationBuilder.endpoint().securityRealm(serverConfigurationBuilder.getSecurityRealm(name));
      return this;
   }

   public List<ProtocolServerConfigurationBuilder<?, ?>> connectors() {
      return connectors;
   }

   public SinglePortServerConfigurationBuilder singlePort() {
      return singlePort;
   }

   public <T extends ProtocolServerConfigurationBuilder<?, ?>> T addConnector(Class<T> klass) {
      try {
         T builder = klass.getConstructor().newInstance();
         connectors.add(builder);
         singlePort.applyConfigurationToProtocol(builder);
         return builder;
      } catch (Exception e) {
         throw Server.log.cannotInstantiateProtocolServerConfigurationBuilder(klass, e);
      }
   }

   @Override
   public void validate() {
   }

   @Override
   public EndpointsConfiguration create() {
      List<ProtocolServerConfiguration> configs = connectors.stream().map(b -> b.create()).collect(Collectors.toList());
      return new EndpointsConfiguration(attributes.protect(), configs, singlePort.create());
   }

   @Override
   public EndpointsConfigurationBuilder read(EndpointsConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
