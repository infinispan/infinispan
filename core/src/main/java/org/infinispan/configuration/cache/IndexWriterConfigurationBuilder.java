package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_COMMIT_INTERVAL;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_LOW_LEVEL_TRACE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_MAX_BUFFERED_ENTRIES;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_QUEUE_COUNT;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_QUEUE_SIZE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_RAM_BUFFER_SIZE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_THREAD_POOL_SIZE;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @since 12.0
 */
public class IndexWriterConfigurationBuilder extends AbstractIndexingConfigurationChildBuilder
      implements Builder<IndexWriterConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;
   private final IndexMergeConfigurationBuilder indexMergeConfigurationBuilder;
   private final Attribute<Integer> commitInterval;
   private final Attribute<Integer> threadPoolSize;
   private final Attribute<Integer> queueCount;
   private final Attribute<Integer> queueSize;
   private final Attribute<Integer> ramBufferSize;
   private final Attribute<Integer> maxBufferedEntries;
   private final Attribute<Boolean> lowLevelTrace;
   private final List<ConfigurationBuilderInfo> subElements;

   IndexWriterConfigurationBuilder(IndexingConfigurationBuilder builder) {
      super(builder);
      this.attributes = IndexWriterConfiguration.attributeDefinitionSet();
      this.threadPoolSize = attributes.attribute(INDEX_THREAD_POOL_SIZE);
      this.queueCount = attributes.attribute(INDEX_QUEUE_COUNT);
      this.queueSize = attributes.attribute(INDEX_QUEUE_SIZE);
      this.commitInterval = attributes.attribute(INDEX_COMMIT_INTERVAL);
      this.ramBufferSize = attributes.attribute(INDEX_RAM_BUFFER_SIZE);
      this.maxBufferedEntries = attributes.attribute(INDEX_MAX_BUFFERED_ENTRIES);
      this.lowLevelTrace = attributes.attribute(INDEX_LOW_LEVEL_TRACE);
      this.indexMergeConfigurationBuilder = new IndexMergeConfigurationBuilder(builder);
      this.subElements = Collections.singletonList(indexMergeConfigurationBuilder);
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   public IndexMergeConfigurationBuilder merge() {
      return indexMergeConfigurationBuilder;
   }

   public IndexWriterConfigurationBuilder threadPoolSize(int value) {
      threadPoolSize.set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder queueCount(int value) {
      queueCount.set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder queueSize(int value) {
      queueSize.set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder commitInterval(int value) {
      commitInterval.set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder ramBufferSize(int value) {
      ramBufferSize.set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder maxBufferedEntries(int value) {
      maxBufferedEntries.set(value);
      return this;
   }

   public IndexWriterConfigurationBuilder setLowLevelTrace(boolean value) {
      lowLevelTrace.set(value);
      return this;
   }

   @Override
   public ElementDefinition<IndexWriterConfiguration> getElementDefinition() {
      return IndexWriterConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public IndexWriterConfiguration create() {
      return new IndexWriterConfiguration(attributes.protect(), indexMergeConfigurationBuilder.create());
   }

   @Override
   public IndexWriterConfigurationBuilder read(IndexWriterConfiguration template) {
      this.attributes.read(template.attributes());
      this.indexMergeConfigurationBuilder.read(template.merge());
      return this;
   }

   @Override
   public String toString() {
      return "IndexWriterConfigurationBuilder{" +
            "attributes=" + attributes +
            ", indexMergeConfigurationBuilder=" + indexMergeConfigurationBuilder +
            '}';
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

}
