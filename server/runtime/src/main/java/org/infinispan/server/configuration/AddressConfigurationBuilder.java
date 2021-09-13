package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.AddressConfiguration.VALUE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
class AddressConfigurationBuilder implements Builder<AddressConfiguration> {

   private final AttributeSet attributes;
   private AddressType addressType;

   AddressConfigurationBuilder() {
      this.attributes = AddressConfiguration.attributeDefinitionSet();
   }

   AddressConfigurationBuilder type(AddressType addressType, String value) {
      this.addressType = addressType;
      attributes.attribute(VALUE).set(value);
      return this;
   }

   AddressType addressType() {
      return addressType;
   }

   public String value() {
      return attributes.attribute(VALUE).get();
   }

   @Override
   public AddressConfiguration create() {
      return new AddressConfiguration(attributes.protect(), addressType);
   }

   @Override
   public AddressConfigurationBuilder read(AddressConfiguration template) {
      this.attributes.read(template.attributes());
      this.addressType = template.addressType();
      return this;
   }
}
