package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_COMMIT_INTERVAL;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_LOW_LEVEL_TRACE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_MAX_BUFFERED_ENTRIES;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_QUEUE_COUNT;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_QUEUE_SIZE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_RAM_BUFFER_SIZE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_THREAD_POOL_SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 12.0
 */
public class IndexWriterConfigurationBuilder extends AbstractIndexingConfigurationChildBuilder
      implements Builder<IndexWriterConfiguration> {

   private final AttributeSet attributes;
   private final IndexMergeConfigurationBuilder indexMergeConfigurationBuilder;

   IndexWriterConfigurationBuilder(IndexingConfigurationBuilder builder) {
      super(builder);
      this.attributes = IndexWriterConfiguration.attributeDefinitionSet();
      this.indexMergeConfigurationBuilder = new IndexMergeConfigurationBuilder(builder);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public IndexMergeConfigurationBuilder merge() {
      return indexMergeConfigurationBuilder;
   }

   public IndexWriterConfigurationBuilder threadPoolSize(int value) {
      attributes.attribute(INDEX_THREAD_POOL_SIZE).set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder queueCount(int value) {
      attributes.attribute(INDEX_QUEUE_COUNT).set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder queueSize(int value) {
      attributes.attribute(INDEX_QUEUE_SIZE).set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder commitInterval(int value) {
      attributes.attribute(INDEX_COMMIT_INTERVAL).set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder ramBufferSize(int value) {
      attributes.attribute(INDEX_RAM_BUFFER_SIZE).set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder maxBufferedEntries(int value) {
      attributes.attribute(INDEX_MAX_BUFFERED_ENTRIES).set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder setLowLevelTrace(boolean value) {
      attributes.attribute(INDEX_LOW_LEVEL_TRACE).set(value);
      return this;
   }

   @Override
   public IndexWriterConfiguration create() {
      return new IndexWriterConfiguration(attributes.protect(), indexMergeConfigurationBuilder.create());
   }

   @Override
   public IndexWriterConfigurationBuilder read(IndexWriterConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.indexMergeConfigurationBuilder.read(template.merge(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "IndexWriterConfigurationBuilder{" +
            "attributes=" + attributes +
            ", indexMergeConfigurationBuilder=" + indexMergeConfigurationBuilder +
            '}';
   }
}
