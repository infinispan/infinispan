package org.infinispan.persistence.jdbc.configuration;


import static org.infinispan.persistence.jdbc.configuration.SegmentColumnConfiguration.SEGMENT_COLUMN_NAME;
import static org.infinispan.persistence.jdbc.configuration.SegmentColumnConfiguration.SEGMENT_COLUMN_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;

public class SegmentColumnConfigurationBuilder implements Builder<SegmentColumnConfiguration> {

   private final AttributeSet attributes;
   private final AbstractJdbcStoreConfigurationBuilder abstractJdbcStoreConfigurationBuilder;

   SegmentColumnConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder abstractJdbcStoreConfigurationBuilder) {
      this.abstractJdbcStoreConfigurationBuilder = abstractJdbcStoreConfigurationBuilder;
      attributes = SegmentColumnConfiguration.attributeSet();
   }

   public SegmentColumnConfigurationBuilder columnName(String columnName) {
      attributes.attribute(SEGMENT_COLUMN_NAME).set(columnName);
      return this;
   }

   public SegmentColumnConfigurationBuilder columnType(String columnType) {
      attributes.attribute(SEGMENT_COLUMN_TYPE).set(columnType);
      return this;
   }

   @Override
   public void validate() {
      Boolean segmented = abstractJdbcStoreConfigurationBuilder.attributes().attribute(AbstractStoreConfiguration.SEGMENTED).get();
      if (segmented != null && segmented) {
         TableManipulationConfigurationBuilder.validateIfSet(attributes, SEGMENT_COLUMN_NAME, SEGMENT_COLUMN_TYPE);
      }
   }

   @Override
   public SegmentColumnConfiguration create() {
      return new SegmentColumnConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(SegmentColumnConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
