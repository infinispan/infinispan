package org.infinispan.cdc.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public record ColumnConfiguration(AttributeSet attributes) {

   static final AttributeDefinition<String> COLUMN_NAME = AttributeDefinition
         .builder(Attribute.NAME, null, String.class)
         .immutable()
         .build();

   static AttributeSet attributeSet() {
      return new AttributeSet(ColumnConfiguration.class, COLUMN_NAME);
   }

   public String name() {
      return attributes.attribute(COLUMN_NAME).get();
   }
}
