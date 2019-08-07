package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
class AddressConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> VALUE = AttributeDefinition.builder("value", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AddressConfiguration.class, VALUE);
   }

   private final ElementDefinition<ConfigurationInfo> elementDefinition;
   private final AttributeSet attributes;
   private final AddressType addressType;

   AddressConfiguration(AttributeSet attributes, AddressType addressType) {
      this.attributes = attributes.checkProtection();
      this.addressType = addressType;
      this.elementDefinition = new DefaultElementDefinition<>(addressType.displayName(), true, false);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   AddressType addressType() {
      return addressType;
   }

   public String value() {
      return attributes.attribute(VALUE).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AddressConfiguration that = (AddressConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      return addressType == that.addressType;
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + addressType.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "AddressConfiguration{" +
            "attributes=" + attributes +
            ", addressType=" + addressType +
            '}';
   }

}
