package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.TimestampColumnConfiguration.TIMESTAMP_COLUMN_NAME;
import static org.infinispan.persistence.jdbc.configuration.TimestampColumnConfiguration.TIMESTAMP_COLUMN_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class TimestampColumnConfigurationBuilder implements Builder<TimestampColumnConfiguration> {

   private final AttributeSet attributes;

   TimestampColumnConfigurationBuilder() {
      attributes = TimestampColumnConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public TimestampColumnConfigurationBuilder dataColumnName(String dataColumnName) {
      attributes.attribute(TIMESTAMP_COLUMN_NAME).set(dataColumnName);
      return this;
   }

   public TimestampColumnConfigurationBuilder dataColumnType(String dataColumnType) {
      attributes.attribute(TIMESTAMP_COLUMN_TYPE).set(dataColumnType);
      return this;
   }

   @Override
   public void validate() {
      TableManipulationConfigurationBuilder.validateIfSet(attributes, TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE);
   }

   @Override
   public TimestampColumnConfiguration create() {
      return new TimestampColumnConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TimestampColumnConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

}
