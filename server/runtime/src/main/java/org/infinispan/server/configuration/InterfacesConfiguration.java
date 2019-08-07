package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.network.NetworkAddress;

public class InterfacesConfiguration implements ConfigurationInfo {
   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.INTERFACES.toString());
   private final List<ConfigurationInfo> subElement = new ArrayList<>();
   private final Map<String, NetworkAddress> addresses;
   private final List<InterfaceConfiguration> interfaces;

   Map<String, NetworkAddress> getAddressMap() {
      return addresses;
   }

   InterfacesConfiguration(List<InterfaceConfiguration> interfaces) {
      Map<String, NetworkAddress> addressMap = interfaces.stream()
            .collect(Collectors.toMap(InterfaceConfiguration::name, InterfaceConfiguration::getNetworkAddress));
      this.addresses = Collections.unmodifiableMap(addressMap);
      this.subElement.addAll(interfaces);
      this.interfaces = interfaces;
   }

   public List<InterfaceConfiguration> interfaces() {
      return interfaces;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElement;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }
}
