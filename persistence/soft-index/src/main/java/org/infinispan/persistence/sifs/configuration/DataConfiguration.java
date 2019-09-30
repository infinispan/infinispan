package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.Element.DATA;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class DataConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> DATA_LOCATION = AttributeDefinition.builder("dataLocation", null, String.class).immutable().autoPersist(false).xmlName("path").build();
   public static final AttributeDefinition<Integer> MAX_FILE_SIZE = AttributeDefinition.builder("maxFileSize", 16 * 1024 * 1024).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> SYNC_WRITES = AttributeDefinition.builder("syncWrites", false).immutable().autoPersist(false).build();
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DataConfiguration.class, DATA_LOCATION, MAX_FILE_SIZE, SYNC_WRITES);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(DATA.getLocalName());

   DataConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
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
