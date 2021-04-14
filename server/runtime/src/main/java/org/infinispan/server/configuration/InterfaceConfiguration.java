package org.infinispan.server.configuration;

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

   NetworkAddress getNetworkAddress() {
      return networkAddress;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InterfaceConfiguration that = (InterfaceConfiguration) o;

      if (!addressConfiguration.equals(that.addressConfiguration)) return false;
      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      int result = addressConfiguration.hashCode();
      result = 31 * result + attributes.hashCode();
      return result;
   }
}
