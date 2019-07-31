package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.TIMESTAMP_COLUMN;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class TimestampColumnConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_NAME = AttributeDefinition.builder("timestampColumnName", null, String.class).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_TYPE = AttributeDefinition.builder("timestampColumnType", null, String.class).xmlName("type").immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TimestampColumnConfiguration.class, TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(TIMESTAMP_COLUMN.getLocalName());

   private final Attribute<String> timestampColumnName;
   private final Attribute<String> timestampColumnType;

   private final AttributeSet attributes;

   public TimestampColumnConfiguration(AttributeSet attributes) {
      timestampColumnName = attributes.attribute(TIMESTAMP_COLUMN_NAME);
      timestampColumnType = attributes.attribute(TIMESTAMP_COLUMN_TYPE);
      this.attributes = attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
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
