package org.infinispan.cdc.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class ColumnConfigurationBuilder implements Builder<ColumnConfiguration> {

   private final AttributeSet attributes;

   ColumnConfigurationBuilder() {
      attributes = ColumnConfiguration.attributeSet();
   }

   public ColumnConfigurationBuilder name(String name) {
      attributes.attribute(Attribute.NAME).set(name);
      return this;
   }

   @Override
   public ColumnConfiguration create() {
      return new ColumnConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(ColumnConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
