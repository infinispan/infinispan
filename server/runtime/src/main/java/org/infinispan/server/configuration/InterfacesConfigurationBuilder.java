package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class InterfacesConfigurationBuilder implements Builder<InterfacesConfiguration> {

   private final Map<String, InterfaceConfigurationBuilder> interfaces = new HashMap<>(2);

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   InterfaceConfigurationBuilder addInterface(String name) {
      InterfaceConfigurationBuilder interfaceConfigurationBuilder = new InterfaceConfigurationBuilder(name);
      interfaces.put(name, interfaceConfigurationBuilder);
      return interfaceConfigurationBuilder;
   }

   @Override
   public InterfacesConfiguration create() {
      Map<String, InterfaceConfiguration> configurations = interfaces.values().stream().collect(Collectors.toMap(e -> e.name(), InterfaceConfigurationBuilder::create));
      return new InterfacesConfiguration(configurations);
   }

   @Override
   public InterfacesConfigurationBuilder read(InterfacesConfiguration template, Combine combine) {
      interfaces.clear();
      template.interfaces().forEach((n, i) -> addInterface(i.name()).read(i, combine));
      return this;
   }
}
