package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Attribute;

public class DataConfiguration {
   public static final AttributeDefinition<String> DATA_LOCATION = AttributeDefinition.builder(Attribute.PATH, null, String.class).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> MAX_FILE_SIZE = AttributeDefinition.builder(Attribute.MAX_FILE_SIZE, 16 * 1024 * 1024).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> SYNC_WRITES = AttributeDefinition.builder(Attribute.SYNC_WRITES, false).immutable().autoPersist(false).build();
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataConfiguration.class, DATA_LOCATION, MAX_FILE_SIZE, SYNC_WRITES);
   }

   DataConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   public AttributeSet attributes() {
      return attributes;
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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataConfiguration that = (DataConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "DataConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
