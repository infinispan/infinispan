package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class BooleanElementConfigurationBuilder implements Builder<BooleanElementConfiguration> {
   private final AttributeSet attributes;
   private final String elementName;
   private final SaslConfigurationBuilder sasl;
   private final String saslProperty;

   BooleanElementConfigurationBuilder(String elementName, SaslConfigurationBuilder sasl, String saslProperty) {
      this.elementName = elementName;
      this.sasl = sasl;
      this.saslProperty = saslProperty;
      this.attributes = BooleanElementConfiguration.attributeDefinitionSet();
   }

   public BooleanElementConfigurationBuilder value(boolean value) {
      attributes.attribute(BooleanElementConfiguration.VALUE).set(value);
      sasl.addMechProperty(saslProperty, String.valueOf(value));
      return this;
   }

   public Boolean value() {
      return attributes.attribute(BooleanElementConfiguration.VALUE).get();
   }

   @Override
   public void validate() {
   }

   @Override
   public BooleanElementConfiguration create() {
      return new BooleanElementConfiguration(attributes.protect(), elementName);
   }

   @Override
   public BooleanElementConfigurationBuilder read(BooleanElementConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
