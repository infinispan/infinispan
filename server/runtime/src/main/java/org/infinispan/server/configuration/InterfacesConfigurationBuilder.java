package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.network.NetworkAddress;

public class InterfacesConfigurationBuilder implements Builder<InterfacesConfiguration> {

   private final Map<String, InterfaceConfigurationBuilder> interfaces = new HashMap<>(2);

   InterfaceConfigurationBuilder addInterface(String name) {
      InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder(name);
      interfaces.put(name, interfaceConfigurationBuilder);
      return interfaceConfigurationBuilder;
   }

   NetworkAddress getNetworkAddress(String interfaceName) {
      return interfaces.get(interfaceName).networkAddress();
   }

   @Override
   public InterfacesConfiguration create() {
      List<InterfaceConfiguration> configurations = interfaces.values().stream()
            .map(InterfaceConfigurationBuilder::create).collect(Collectors.toList());
      return new InterfacesConfiguration(configurations);
   }

   @Override
   public InterfacesConfigurationBuilder read(InterfacesConfiguration template) {
      interfaces.clear();
      template.interfaces().forEach(i -> addInterface(i.name()).read(i));
      return this;
   }

   @Override
   public void validate() {
   }

   boolean exists(String interfaceName) {
      return interfaces.containsKey(interfaceName);
   }
}
