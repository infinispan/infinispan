package org.infinispan.server.configuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.infinispan.server.network.SocketBinding;

public class SocketBindingsConfigurationBuilder implements Builder<SocketBindingsConfiguration> {

   private final AttributeSet attributes;
   private final ServerConfigurationBuilder server;

   private Map<String, SocketBindingConfigurationBuilder> socketBindings = new LinkedHashMap<>(2);

   SocketBindingsConfigurationBuilder(ServerConfigurationBuilder server) {
      this.server = server;
      attributes = SocketBindingsConfiguration.attributeDefinitionSet();
   }

   SocketBindingsConfigurationBuilder socketBinding(String name, int port, String interfaceName) {
      SocketBindingConfigurationBuilder configurationBuilder = new SocketBindingConfigurationBuilder(server);
      configurationBuilder.binding(name, port, interfaceName);
      socketBindings.put(name, configurationBuilder);
      return this;
   }

   @Override
   public void validate() {
   }

   public Map<String, SocketBindingConfigurationBuilder> socketBindings() {
      return socketBindings;
   }

   public SocketBindingsConfigurationBuilder offset(Integer offset) {
      attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).set(offset);
      return this;
   }

   public Integer offset() {
      return attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).get();
   }

   SocketBindingsConfigurationBuilder defaultInterface(String interfaceName) {
      if (!server.interfaces().exists(interfaceName)) {
         throw Server.log.unknownInterface(interfaceName);
      }
      attributes.attribute(SocketBindingsConfiguration.DEFAULT_INTERFACE).set(interfaceName);
      return this;
   }

   boolean exists(String bindingName) {
      return socketBindings.containsKey(bindingName);
   }

   @Override
   public SocketBindingsConfiguration create() {
      List<SocketBindingConfiguration> bindings = socketBindings.values().stream()
            .map(SocketBindingConfigurationBuilder::create).collect(Collectors.toList());
      return new SocketBindingsConfiguration(attributes.protect(), bindings);
   }

   @Override
   public SocketBindingsConfigurationBuilder read(SocketBindingsConfiguration template) {
      this.attributes.read(template.attributes());
      socketBindings.clear();
      template.socketBindings().forEach(s -> socketBinding(s.name(), s.port(), s.interfaceName()));
      return this;
   }

   SocketBinding getSocketBinding(String name) {
      return socketBindings.get(name).getSocketBinding();
   }

   String defaultInterface() {
      return attributes.attribute(SocketBindingsConfiguration.DEFAULT_INTERFACE).get();
   }
}
