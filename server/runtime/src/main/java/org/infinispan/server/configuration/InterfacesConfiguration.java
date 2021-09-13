package org.infinispan.server.configuration;

import java.util.Map;

public class InterfacesConfiguration {
   private final Map<String, InterfaceConfiguration> interfaces;

   InterfacesConfiguration(Map<String, InterfaceConfiguration> interfaces) {
      this.interfaces = interfaces;
   }

   public Map<String, InterfaceConfiguration> interfaces() {
      return interfaces;
   }

   boolean exists(String interfaceName) {
      return interfaces.containsKey(interfaceName);
   }
}
