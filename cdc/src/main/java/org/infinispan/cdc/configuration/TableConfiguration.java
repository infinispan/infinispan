package org.infinispan.cdc.configuration;

import java.util.Collection;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public record TableConfiguration(AttributeSet attributes, ColumnConfiguration primaryKey, Collection<ColumnConfiguration> columns) {

   static final AttributeDefinition<String> TABLE_NAME = AttributeDefinition
         .builder(Attribute.NAME, null, String.class)
         .immutable()
         .build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TableConfiguration.class, TABLE_NAME);
   }

   public String name() {
      return attributes.attribute(TABLE_NAME).get();
   }
}
