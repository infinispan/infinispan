package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.DataConfiguration.DATA_LOCATION;
import static org.infinispan.persistence.sifs.configuration.DataConfiguration.MAX_FILE_SIZE;
import static org.infinispan.persistence.sifs.configuration.DataConfiguration.SYNC_WRITES;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class DataConfigurationBuilder implements Builder<DataConfiguration> {

   private final AttributeSet attributes;

   public DataConfigurationBuilder() {
      this.attributes = DataConfiguration.attributeDefinitionSet();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public DataConfigurationBuilder dataLocation(String dataLocation) {
      attributes.attribute(DATA_LOCATION).set(dataLocation);
      return this;
   }

   public DataConfigurationBuilder maxFileSize(int maxFileSize) {
      attributes.attribute(MAX_FILE_SIZE).set(maxFileSize);
      return this;
   }

   public DataConfigurationBuilder syncWrites(boolean syncWrites) {
      attributes.attribute(SYNC_WRITES).set(syncWrites);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public DataConfiguration create() {
      return new DataConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(DataConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
