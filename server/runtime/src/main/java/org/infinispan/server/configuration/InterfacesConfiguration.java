package org.infinispan.server.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.server.network.NetworkAddress;

public class InterfacesConfiguration {
   private final Map<String, NetworkAddress> addresses;
   private final List<InterfaceConfiguration> interfaces;

   Map<String, NetworkAddress> getAddressMap() {
      return addresses;
   }

   InterfacesConfiguration(List<InterfaceConfiguration> interfaces) {
      Map<String, NetworkAddress> addressMap = interfaces.stream()
            .collect(Collectors.toMap(InterfaceConfiguration::name, InterfaceConfiguration::getNetworkAddress));
      this.addresses = Collections.unmodifiableMap(addressMap);
      this.interfaces = interfaces;
   }

   public List<InterfaceConfiguration> interfaces() {
      return interfaces;
   }
}
