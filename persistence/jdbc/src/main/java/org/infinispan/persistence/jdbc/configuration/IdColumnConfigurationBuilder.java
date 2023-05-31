package org.infinispan.persistence.jdbc.configuration;


import static org.infinispan.persistence.jdbc.configuration.IdColumnConfiguration.ID_COLUMN_NAME;
import static org.infinispan.persistence.jdbc.configuration.IdColumnConfiguration.ID_COLUMN_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class IdColumnConfigurationBuilder implements Builder<IdColumnConfiguration> {

   private final AttributeSet attributes;

   IdColumnConfigurationBuilder() {
      attributes = IdColumnConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public IdColumnConfigurationBuilder idColumnName(String idColumnName) {
      attributes.attribute(ID_COLUMN_NAME).set(idColumnName);
      return this;
   }

   public IdColumnConfigurationBuilder idColumnType(String idColumnType) {
      attributes.attribute(ID_COLUMN_TYPE).set(idColumnType);
      return this;
   }

   @Override
   public void validate() {
      TableManipulationConfigurationBuilder.validateIfSet(attributes, ID_COLUMN_NAME, ID_COLUMN_TYPE);
   }

   @Override
   public IdColumnConfiguration create() {
      return new IdColumnConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(IdColumnConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

}
