package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

public class IndexConfiguration extends ConfigurationElement<IndexConfiguration> {

   public static final AttributeDefinition<String> INDEX_LOCATION = AttributeDefinition.builder(Attribute.PATH, null, String.class).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_LENGTH = AttributeDefinition.builder(Attribute.INDEX_QUEUE_LENGTH, 1000).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> INDEX_SEGMENTS = AttributeDefinition.builder(Attribute.SEGMENTS, 3).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> MIN_NODE_SIZE = AttributeDefinition.builder(Attribute.MIN_NODE_SIZE, 0).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> MAX_NODE_SIZE = AttributeDefinition.builder(Attribute.MAX_NODE_SIZE, 4096).immutable().autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexConfiguration.class, INDEX_LOCATION, INDEX_QUEUE_LENGTH, INDEX_SEGMENTS, MIN_NODE_SIZE, MAX_NODE_SIZE);
   }

   public IndexConfiguration(AttributeSet attributes) {
      super(Element.INDEX, attributes);
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
}
