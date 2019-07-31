package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.ID_COLUMN;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class IdColumnConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<String> ID_COLUMN_NAME = AttributeDefinition.builder("idColumnName", null, String.class).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> ID_COLUMN_TYPE = AttributeDefinition.builder("idColumnType", null, String.class).immutable().xmlName("type").build();

   static AttributeSet attributeSet() {
      return new AttributeSet(IdColumnConfiguration.class, ID_COLUMN_NAME, ID_COLUMN_TYPE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(ID_COLUMN.getLocalName());

   private final Attribute<String> idColumnName;
   private final Attribute<String> idColumnType;
   private final AttributeSet attributes;

   public IdColumnConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      idColumnName = attributes.attribute(ID_COLUMN_NAME);
      idColumnType = attributes.attribute(ID_COLUMN_TYPE);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public String idColumnName() {
      return idColumnName.get();
   }

   public String idColumnType() {
      return idColumnType.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IdColumnConfiguration that = (IdColumnConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "IdColumnConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
