package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class TimestampColumnConfiguration {
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_NAME = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.NAME, null, String.class).immutable().build();
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_TYPE = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.TYPE, null, String.class).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TimestampColumnConfiguration.class, TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE);
   }

   private final Attribute<String> timestampColumnName;
   private final Attribute<String> timestampColumnType;

   private final AttributeSet attributes;

   public TimestampColumnConfiguration(AttributeSet attributes) {
      timestampColumnName = attributes.attribute(TIMESTAMP_COLUMN_NAME);
      timestampColumnType = attributes.attribute(TIMESTAMP_COLUMN_TYPE);
      this.attributes = attributes;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String dataColumnName() {
      return timestampColumnName.get();
   }

   public String dataColumnType() {
      return timestampColumnType.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TimestampColumnConfiguration that = (TimestampColumnConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "TimestampColumnConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
