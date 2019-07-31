package org.infinispan.persistence.jdbc.configuration;


import static org.infinispan.persistence.jdbc.configuration.Element.SEGMENT_COLUMN;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class SegmentColumnConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> SEGMENT_COLUMN_NAME = AttributeDefinition.builder("segmentColumnName", null, String.class).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> SEGMENT_COLUMN_TYPE = AttributeDefinition.builder("segmentColumnType", null, String.class).xmlName("type").immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(SegmentColumnConfiguration.class, SEGMENT_COLUMN_NAME, SEGMENT_COLUMN_TYPE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SEGMENT_COLUMN.getLocalName());

   private final Attribute<String> segmentColumnName;
   private final Attribute<String> segmentColumnType;

   private final AttributeSet attributes;

   public SegmentColumnConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      segmentColumnName = attributes.attribute(SEGMENT_COLUMN_NAME);
      segmentColumnType = attributes.attribute(SEGMENT_COLUMN_TYPE);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String segmentColumnName() {
      return segmentColumnName.get();
   }

   public String segmentColumnType() {
      return segmentColumnType.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SegmentColumnConfiguration that = (SegmentColumnConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "SegmentColumnConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
