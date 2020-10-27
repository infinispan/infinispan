package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.Element.INDEX;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class IndexConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<String> INDEX_LOCATION = AttributeDefinition.builder("indexLocation", null, String.class).immutable().autoPersist(false).xmlName("path").build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_LENGTH = AttributeDefinition.builder("indexQueueLength", 1000).immutable().autoPersist(false).xmlName("max-queue-length").build();
   public static final AttributeDefinition<Integer> INDEX_SEGMENTS = AttributeDefinition.builder("indexSegments", 3).immutable().autoPersist(false).xmlName("segments").build();
   public static final AttributeDefinition<Integer> MIN_NODE_SIZE = AttributeDefinition.builder("minNodeSize", 0).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> MAX_NODE_SIZE = AttributeDefinition.builder("maxNodeSize", 4096).immutable().autoPersist(false).build();

   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexConfiguration.class, INDEX_LOCATION, INDEX_QUEUE_LENGTH, INDEX_SEGMENTS, MIN_NODE_SIZE, MAX_NODE_SIZE);
   }

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(INDEX.getLocalName());

   public IndexConfiguration(AttributeSet attributes) {
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

   public String indexLocation() {
      return attributes.attribute(INDEX_LOCATION).get();
   }

   public void setLocation(String location) {
      attributes.attribute(INDEX_LOCATION).set(location);
   }

   public int indexSegments() {
      return attributes.attribute(INDEX_SEGMENTS).get();
   }

   public int minNodeSize() {
      return attributes.attribute(MIN_NODE_SIZE).get();
   }

   public int maxNodeSize() {
      return attributes.attribute(MAX_NODE_SIZE).get();
   }

   public int indexQueueLength() {
      return attributes.attribute(INDEX_QUEUE_LENGTH).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexConfiguration that = (IndexConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "IndexConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
