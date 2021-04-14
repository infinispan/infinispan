package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 12.0
 */
public class IndexWriterConfiguration extends ConfigurationElement<IndexWriterConfiguration> {

   public static final AttributeDefinition<Integer> INDEX_THREAD_POOL_SIZE =
         AttributeDefinition.builder(Attribute.THREAD_POOL_SIZE, 1, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_COUNT =
         AttributeDefinition.builder(Attribute.QUEUE_COUNT, 1, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_SIZE =
         AttributeDefinition.builder(Attribute.QUEUE_SIZE, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_COMMIT_INTERVAL =
         AttributeDefinition.builder(Attribute.COMMIT_INTERVAL, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_RAM_BUFFER_SIZE =
         AttributeDefinition.builder(Attribute.RAM_BUFFER_SIZE, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_MAX_BUFFERED_ENTRIES =
         AttributeDefinition.builder(Attribute.MAX_BUFFERED_ENTRIES, null, Integer.class).immutable().build();
   public static final AttributeDefinition<Boolean> INDEX_LOW_LEVEL_TRACE =
         AttributeDefinition.builder(Attribute.LOW_LEVEL_TRACE, false, Boolean.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexWriterConfiguration.class, INDEX_THREAD_POOL_SIZE, INDEX_QUEUE_COUNT, INDEX_QUEUE_SIZE,
            INDEX_COMMIT_INTERVAL, INDEX_RAM_BUFFER_SIZE, INDEX_MAX_BUFFERED_ENTRIES, INDEX_LOW_LEVEL_TRACE);
   }

   private final IndexMergeConfiguration indexMergeConfiguration;

   IndexWriterConfiguration(AttributeSet attributes, IndexMergeConfiguration indexMergeConfiguration) {
      super(Element.INDEX_WRITER, attributes, indexMergeConfiguration);
      this.indexMergeConfiguration = indexMergeConfiguration;
   }

   public IndexMergeConfiguration merge() {
      return indexMergeConfiguration;
   }

   public Integer getThreadPoolSize() {
      return attributes.attribute(INDEX_THREAD_POOL_SIZE).get();
   }

   public Integer getQueueCount() {
      return attributes.attribute(INDEX_QUEUE_COUNT).get();
   }

   public Integer getQueueSize() {
      return attributes.attribute(INDEX_QUEUE_SIZE).get();
   }

   public Integer getCommitInterval() {
      return attributes.attribute(INDEX_COMMIT_INTERVAL).get();
   }

   public Integer getRamBufferSize() {
      return attributes.attribute(INDEX_RAM_BUFFER_SIZE).get();
   }

   public Integer getMaxBufferedEntries() {
      return attributes.attribute(INDEX_MAX_BUFFERED_ENTRIES).get();
   }

   public Boolean isLowLevelTrace() {
      return attributes.attribute(INDEX_LOW_LEVEL_TRACE).get();
   }
}
