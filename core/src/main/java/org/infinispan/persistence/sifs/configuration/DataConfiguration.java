package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

public class DataConfiguration extends ConfigurationElement<DataConfiguration> {
   public static final AttributeDefinition<String> DATA_LOCATION = AttributeDefinition.builder(Attribute.PATH, null, String.class).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> MAX_FILE_SIZE = AttributeDefinition.builder(Attribute.MAX_FILE_SIZE, 16 * 1024 * 1024).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> SYNC_WRITES = AttributeDefinition.builder(Attribute.SYNC_WRITES, false).immutable().autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataConfiguration.class, DATA_LOCATION, MAX_FILE_SIZE, SYNC_WRITES);
   }

   DataConfiguration(AttributeSet attributes) {
      super(Element.DATA, attributes);
   }

   public int maxFileSize() {
      return attributes.attribute(MAX_FILE_SIZE).get();
   }

   public boolean syncWrites() {
      return attributes.attribute(SYNC_WRITES).get();
   }

   public String dataLocation() {
      return attributes.attribute(DATA_LOCATION).get();
   }

   public void setDataLocation(String newLocation) {
      attributes.attribute(DATA_LOCATION).set(newLocation);
   }
}
