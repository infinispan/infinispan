package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.DATA_COLUMN;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class DataColumnConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> DATA_COLUMN_NAME = AttributeDefinition.builder("dataColumnName", null, String.class).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> DATA_COLUMN_TYPE = AttributeDefinition.builder("dataColumnType", null, String.class).immutable().xmlName("type").build();

   static AttributeSet attributeSet() {
      return new AttributeSet(DataColumnConfiguration.class, DATA_COLUMN_NAME, DATA_COLUMN_TYPE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(DATA_COLUMN.getLocalName());

   private final Attribute<String> dataColumnName;
   private final Attribute<String> dataColumnType;

   private final AttributeSet attributes;

   public DataColumnConfiguration(AttributeSet attributes) {
      dataColumnName = attributes.attribute(DATA_COLUMN_NAME);
      dataColumnType = attributes.attribute(DATA_COLUMN_TYPE);
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
