package org.infinispan.persistence.jdbc.configuration;


import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class SegmentColumnConfiguration {
   public static final AttributeDefinition<String> SEGMENT_COLUMN_NAME = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.NAME, null, String.class).immutable().build();
   public static final AttributeDefinition<String> SEGMENT_COLUMN_TYPE = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.TYPE, null, String.class).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(SegmentColumnConfiguration.class, SEGMENT_COLUMN_NAME, SEGMENT_COLUMN_TYPE);
   }

   private final Attribute<String> segmentColumnName;
   private final Attribute<String> segmentColumnType;

   private final AttributeSet attributes;

   public SegmentColumnConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      segmentColumnName = attributes.attribute(SEGMENT_COLUMN_NAME);
      segmentColumnType = attributes.attribute(SEGMENT_COLUMN_TYPE);
   }

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
