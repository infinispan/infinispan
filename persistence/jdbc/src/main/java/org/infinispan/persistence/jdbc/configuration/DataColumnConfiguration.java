package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class DataColumnConfiguration {
   public static final AttributeDefinition<String> DATA_COLUMN_NAME = AttributeDefinition.builder(org.infinispan.persistence.jdbc.configuration.Attribute.NAME, null, String.class).immutable().build();
   public static final AttributeDefinition<String> DATA_COLUMN_TYPE = AttributeDefinition.builder(org.infinispan.persistence.jdbc.configuration.Attribute.TYPE, null, String.class).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(DataColumnConfiguration.class, DATA_COLUMN_NAME, DATA_COLUMN_TYPE);
   }

   private final Attribute<String> dataColumnName;
   private final Attribute<String> dataColumnType;

   private final AttributeSet attributes;

   public DataColumnConfiguration(AttributeSet attributes) {
      dataColumnName = attributes.attribute(DATA_COLUMN_NAME);
      dataColumnType = attributes.attribute(DATA_COLUMN_TYPE);
      this.attributes = attributes;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String dataColumnName() {
      return dataColumnName.get();
   }

   public String dataColumnType() {
      return dataColumnType.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataColumnConfiguration that = (DataColumnConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "DataColumnConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
