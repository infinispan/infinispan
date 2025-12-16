package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.InterfaceConfiguration.NAME;

import java.io.IOException;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.NetworkAddress;

public class InterfaceConfigurationBuilder implements Builder<InterfaceConfiguration> {
   private final AttributeSet attributes;
   private final AddressConfigurationBuilder address = new AddressConfigurationBuilder();
   private NetworkAddress networkAddress;

   InterfaceConfigurationBuilder(String name) {
      this.attributes = InterfaceConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
      return switch (addressType) {
         case ANY_ADDRESS -> NetworkAddress.anyAddress(interfaceName);
         case INET_ADDRESS -> NetworkAddress.fromString(interfaceName, addressValue);
         case LINK_LOCAL -> NetworkAddress.linkLocalAddress(interfaceName);
         case GLOBAL -> NetworkAddress.globalAddress(interfaceName);
         case LOOPBACK -> NetworkAddress.loopback(interfaceName);
         case NON_LOOPBACK -> NetworkAddress.nonLoopback(interfaceName);
         case SITE_LOCAL -> NetworkAddress.siteLocal(interfaceName);
         case MATCH_INTERFACE -> NetworkAddress.matchInterface(interfaceName, addressValue);
         case MATCH_ADDRESS -> NetworkAddress.matchAddress(interfaceName, addressValue);
         case MATCH_HOST -> NetworkAddress.matchHost(interfaceName, addressValue);
      };
   }

   @Override
   public InterfaceConfiguration create() {
      return new InterfaceConfiguration(attributes.protect(), address.create(), networkAddress);
   }

   @Override
   public InterfaceConfigurationBuilder read(InterfaceConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.address.read(template.addressConfiguration(), combine);
      return this;
   }
}
