package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.IndexConfiguration.INDEX_LOCATION;
import static org.infinispan.persistence.sifs.configuration.IndexConfiguration.INDEX_QUEUE_LENGTH;
import static org.infinispan.persistence.sifs.configuration.IndexConfiguration.INDEX_SEGMENTS;
import static org.infinispan.persistence.sifs.configuration.IndexConfiguration.MAX_NODE_SIZE;
import static org.infinispan.persistence.sifs.configuration.IndexConfiguration.MIN_NODE_SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.sifs.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 10.0
 */
public class IndexConfigurationBuilder implements Builder<IndexConfiguration> {

   private static final Log log = LogFactory.getLog(IndexConfigurationBuilder.class, Log.class);

   private final AttributeSet attributes;

   public IndexConfigurationBuilder() {
      this.attributes = IndexConfiguration.attributeDefinitionSet();
   }

   public AttributeSet attributes() {
      return attributes;
   }


   public IndexConfigurationBuilder indexLocation(String indexLocation) {
      attributes.attribute(INDEX_LOCATION).set(indexLocation);
      return this;
   }

   public IndexConfigurationBuilder indexSegments(int indexSegments) {
      attributes.attribute(INDEX_SEGMENTS).set(indexSegments);
      return this;
   }

   public IndexConfigurationBuilder minNodeSize(int minNodeSize) {
      attributes.attribute(MIN_NODE_SIZE).set(minNodeSize);
      return this;
   }

   public IndexConfigurationBuilder maxNodeSize(int maxNodeSize) {
      attributes.attribute(MAX_NODE_SIZE).set(maxNodeSize);
      return this;
   }

   public IndexConfigurationBuilder indexQueueLength(int indexQueueLength) {
      attributes.attribute(INDEX_QUEUE_LENGTH).set(indexQueueLength);
      return this;
   }

   @Override
   public IndexConfiguration create() {
      return new IndexConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(IndexConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public void validate() {
      int minNodeSize = attributes.attribute(MIN_NODE_SIZE).get();
      int maxNodeSize = attributes.attribute(MAX_NODE_SIZE).get();
      if (maxNodeSize <= 0 || maxNodeSize > Short.MAX_VALUE) {
         throw log.maxNodeSizeLimitedToShort(maxNodeSize);
      } else if (minNodeSize < 0 || minNodeSize > maxNodeSize) {
         throw log.minNodeSizeMustBeLessOrEqualToMax(minNodeSize, maxNodeSize);
      }
   }

   @Override
   public String toString() {
      return "IndexConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }
}
