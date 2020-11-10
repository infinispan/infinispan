package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.INDEX_WRITER;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 12.0
 */
public class IndexWriterConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Integer> INDEX_THREAD_POOL_SIZE =
         AttributeDefinition.builder("thread-pool-size", 1, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_COUNT =
         AttributeDefinition.builder("queue-count", 1, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_SIZE =
         AttributeDefinition.builder("queue-size", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_COMMIT_INTERVAL =
         AttributeDefinition.builder("commit-interval", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_RAM_BUFFER_SIZE =
         AttributeDefinition.builder("ram-buffer-size", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Integer> INDEX_MAX_BUFFERED_DOCS =
         AttributeDefinition.builder("max-buffered-docs", null, Integer.class).immutable().build();
   public static final AttributeDefinition<Boolean> INDEX_LOW_LEVEL_TRACE =
         AttributeDefinition.builder("low-level-trace", false, Boolean.class).immutable().build();

   private final List<ConfigurationInfo> subElements;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(IndexWriterConfiguration.class, INDEX_THREAD_POOL_SIZE, INDEX_QUEUE_COUNT, INDEX_QUEUE_SIZE,
            INDEX_COMMIT_INTERVAL, INDEX_RAM_BUFFER_SIZE, INDEX_MAX_BUFFERED_DOCS, INDEX_LOW_LEVEL_TRACE);
   }

   static final ElementDefinition<IndexWriterConfiguration> ELEMENT_DEFINITION =
         new DefaultElementDefinition<>(INDEX_WRITER.getLocalName());

   private final AttributeSet attributes;

   private final IndexMergeConfiguration indexMergeConfiguration;

   IndexWriterConfiguration(AttributeSet attributes, IndexMergeConfiguration indexMergeConfiguration) {
      this.attributes = attributes.checkProtection();
      this.indexMergeConfiguration = indexMergeConfiguration;
      this.subElements = Collections.singletonList(indexMergeConfiguration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   @Override
   public ElementDefinition<IndexWriterConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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

   public Integer getMaxBufferedDocs() {
      return attributes.attribute(INDEX_MAX_BUFFERED_DOCS).get();
   }

   public Boolean isLowLevelTrace() {
      return attributes.attribute(INDEX_LOW_LEVEL_TRACE).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexWriterConfiguration that = (IndexWriterConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      return indexMergeConfiguration.equals(that.indexMergeConfiguration);
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + indexMergeConfiguration.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "IndexWriterConfiguration{" +
            "attributes=" + attributes +
            ", indexMergeConfiguration=" + indexMergeConfiguration +
            '}';
   }
}
