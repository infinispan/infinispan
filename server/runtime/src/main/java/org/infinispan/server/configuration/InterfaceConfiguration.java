package org.infinispan.server.configuration;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.network.NetworkAddress;

/**
 * @since 10.0
 */
public class InterfaceConfiguration extends ConfigurationElement<InterfaceConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();

   private final AddressConfiguration addressConfiguration;
   private final NetworkAddress networkAddress;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(InterfaceConfiguration.class, NAME);
   }

   InterfaceConfiguration(AttributeSet attributes, AddressConfiguration addressConfiguration, NetworkAddress networkAddress) {
      super(Element.INTERFACE, attributes);
      this.addressConfiguration = addressConfiguration;
      this.networkAddress = networkAddress;
   }

   AddressConfiguration addressConfiguration() {
      return addressConfiguration;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      InterfaceConfiguration that = (InterfaceConfiguration) o;
      return Objects.equals(addressConfiguration, that.addressConfiguration) && Objects.equals(networkAddress, that.networkAddress);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), addressConfiguration, networkAddress);
   }

   NetworkAddress getNetworkAddress() {
      return networkAddress;
   }

}
