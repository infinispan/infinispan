package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.TimestampColumnConfiguration.TIMESTAMP_COLUMN_NAME;
import static org.infinispan.persistence.jdbc.configuration.TimestampColumnConfiguration.TIMESTAMP_COLUMN_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class TimestampColumnConfigurationBuilder implements Builder<TimestampColumnConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;

   TimestampColumnConfigurationBuilder() {
      attributes = TimestampColumnConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return TimestampColumnConfiguration.ELEMENT_DEFINITION;
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
   public Builder<?> read(TimestampColumnConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
