package org.infinispan.cdc.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures a single column to use in the change data capture.
 *
 * <p>
 * Describes which columns to include from change events.
 * </p>
 *
 * @param attributes The configuration attributes.
 */
public record ColumnConfiguration(AttributeSet attributes) {

   /**
    * The column name as defined in the database.
    */
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
