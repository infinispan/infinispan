package org.infinispan.server.configuration;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.network.NetworkAddress;

/**
 * @since 10.0
 */
public class InterfaceConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();

   private final AddressConfiguration addressConfiguration;
   private final NetworkAddress networkAddress;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(InterfaceConfiguration.class, NAME);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.INTERFACE.toString());

   private final AttributeSet attributes;

   InterfaceConfiguration(AttributeSet attributes, AddressConfiguration addressConfiguration, NetworkAddress networkAddress) {
      this.attributes = attributes.checkProtection();
      this.addressConfiguration = addressConfiguration;
      this.networkAddress = networkAddress;
   }

   AddressConfiguration addressConfiguration() {
      return addressConfiguration;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return Collections.singletonList(addressConfiguration);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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
