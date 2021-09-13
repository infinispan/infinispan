package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;

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

   public SocketBindingsConfigurationBuilder offset(Integer offset) {
      attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).set(offset);
      return this;
   }

   public Integer offset() {
      return attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).get();
   }

   SocketBindingsConfigurationBuilder defaultInterface(String interfaceName) {
      attributes.attribute(SocketBindingsConfiguration.DEFAULT_INTERFACE).set(interfaceName);
      return this;
   }

   @Override
   public SocketBindingsConfiguration create() {
      throw new UnsupportedOperationException();
   }

   public SocketBindingsConfiguration create(InterfacesConfiguration interfaces) {
      Map<String, SocketBindingConfiguration> bindings = new HashMap<>();
      for(Map.Entry<String, SocketBindingConfigurationBuilder> e : socketBindings.entrySet()) {
         String name = e.getValue().interfaceName();
         if (!interfaces.exists(name)) {
            throw Server.log.unknownInterface(name);
         }
         bindings.put(e.getKey(), e.getValue().create(interfaces.interfaces().get(name)));
      }
      return new SocketBindingsConfiguration(attributes.protect(), bindings);
   }

   @Override
   public SocketBindingsConfigurationBuilder read(SocketBindingsConfiguration template) {
      this.attributes.read(template.attributes());
      socketBindings.clear();
      template.socketBindings().forEach((n, s) -> socketBinding(s.name(), s.port(), s.interfaceName()));
      return this;
   }

   String defaultInterface() {
      return attributes.attribute(SocketBindingsConfiguration.DEFAULT_INTERFACE).get();
   }


}
