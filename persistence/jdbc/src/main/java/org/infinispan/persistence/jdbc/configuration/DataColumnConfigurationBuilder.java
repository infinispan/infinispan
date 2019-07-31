package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.DataColumnConfiguration.DATA_COLUMN_NAME;
import static org.infinispan.persistence.jdbc.configuration.DataColumnConfiguration.DATA_COLUMN_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class DataColumnConfigurationBuilder implements Builder<DataColumnConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;

   DataColumnConfigurationBuilder() {
      attributes = DataColumnConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return DataColumnConfiguration.ELEMENT_DEFINITION;
   }

   public DataColumnConfigurationBuilder dataColumnName(String dataColumnName) {
      attributes.attribute(DATA_COLUMN_NAME).set(dataColumnName);
      return this;
   }

   public DataColumnConfigurationBuilder dataColumnType(String dataColumnType) {
      attributes.attribute(DATA_COLUMN_TYPE).set(dataColumnType);
      return this;
   }

   @Override
   public void validate() {
      TableManipulationConfigurationBuilder.validateIfSet(attributes, DATA_COLUMN_NAME, DATA_COLUMN_TYPE);
   }

   @Override
   public DataColumnConfiguration create() {
      return new DataColumnConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(DataColumnConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
