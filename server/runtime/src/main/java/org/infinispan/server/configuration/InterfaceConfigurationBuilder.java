package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.InterfaceConfiguration.NAME;

import java.io.IOException;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.network.NetworkAddress;

public class InterfaceConfigurationBuilder implements Builder<InterfaceConfiguration> {
   private final AttributeSet attributes;
   private final AddressConfigurationBuilder address = new AddressConfigurationBuilder();
   private NetworkAddress networkAddress;

   InterfaceConfigurationBuilder(String name) {
      this.attributes = InterfaceConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public InterfaceConfigurationBuilder address(AddressType addressType, String addressValue) throws IOException {
      address.type(addressType, addressValue);
      this.networkAddress = createNetworkAddress();
      return this;
   }

   NetworkAddress networkAddress() {
      return networkAddress;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   private NetworkAddress createNetworkAddress() throws IOException {
      String interfaceName = this.name();
      AddressType addressType = address.addressType();
      String addressValue = address.value();
      switch (addressType) {
         case ANY_ADDRESS:
            return NetworkAddress.anyAddress(interfaceName);
         case INET_ADDRESS:
            return NetworkAddress.fromString(interfaceName, addressValue);
         case LINK_LOCAL:
            return NetworkAddress.linkLocalAddress(interfaceName);
         case GLOBAL:
            return NetworkAddress.globalAddress(interfaceName);
         case LOOPBACK:
            return NetworkAddress.loopback(interfaceName);
         case NON_LOOPBACK:
            return NetworkAddress.nonLoopback(interfaceName);
         case SITE_LOCAL:
            return NetworkAddress.siteLocal(interfaceName);
         case MATCH_INTERFACE:
            return NetworkAddress.matchInterface(interfaceName, addressValue);
         case MATCH_ADDRESS:
            return NetworkAddress.matchAddress(interfaceName, addressValue);
         case MATCH_HOST:
            return NetworkAddress.matchHost(interfaceName, addressValue);
      }
      return null;
   }

   @Override
   public InterfaceConfiguration create() {
      return new InterfaceConfiguration(attributes.protect(), address.create(), networkAddress);
   }

   @Override
   public InterfaceConfigurationBuilder read(InterfaceConfiguration template) {
      this.attributes.read(template.attributes());
      this.address.read(template.addressConfiguration());
      return this;
   }
}
