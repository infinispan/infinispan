package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_COMMIT_INTERVAL;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_LOW_LEVEL_TRACE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_MAX_BUFFERED_DOCS;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_QUEUE_COUNT;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_QUEUE_SIZE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_RAM_BUFFER_SIZE;
import static org.infinispan.configuration.cache.IndexWriterConfiguration.INDEX_THREAD_POOL_SIZE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

   private static final String BACKEND_PREFIX = "hibernate.search.backend.";
   private static final String QUEUE_COUNT_KEY = BACKEND_PREFIX + "indexing.queue_count";
   private static final String QUEUE_SIZE_KEY = BACKEND_PREFIX + "indexing.queue_size";
   private static final String THREAD_POOL_KEY = BACKEND_PREFIX + "thread_pool.size";

   private static final String IO_PREFIX = "hibernate.search.backend.io.";
   private static final String COMMIT_INTERVAL_KEY = IO_PREFIX + "commit_interval";
   private static final String RAM_BUFFER_KEY = IO_PREFIX + "writer.ram_buffer_size";
   private static final String MAX_BUFFER_DOCS_KEY = IO_PREFIX + "writer.max_buffered_docs";
   private static final String LOW_LEVEL_TRACE_KEY = IO_PREFIX + "writer.infostream";

   private final AttributeSet attributes;
   private final IndexMergeConfigurationBuilder indexMergeConfigurationBuilder;
   private final Attribute<Integer> commitInterval;
   private final Attribute<Integer> threadPoolSize;
   private final Attribute<Integer> queueCount;
   private final Attribute<Integer> queueSize;
   private final Attribute<Integer> ramBufferSize;
   private final Attribute<Integer> maxBufferedDocs;
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
      this.maxBufferedDocs = attributes.attribute(INDEX_MAX_BUFFERED_DOCS);
      this.lowLevelTrace = attributes.attribute(INDEX_LOW_LEVEL_TRACE);
      this.indexMergeConfigurationBuilder = new IndexMergeConfigurationBuilder(builder);
      this.subElements = Collections.singletonList(indexMergeConfigurationBuilder);
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   Map<String, Object> asInternalProperties() {
      Map<String, Object> props = new HashMap<>();
      if (!commitInterval.isNull()) {
         props.put(COMMIT_INTERVAL_KEY, commitInterval());
      }
      if (!threadPoolSize.isNull()) {
         props.put(THREAD_POOL_KEY, threadPoolSize());
      }
      if (!queueCount.isNull()) {
         props.put(QUEUE_COUNT_KEY, queueCount());
      }
      if (!queueSize.isNull()) {
         props.put(QUEUE_SIZE_KEY, queueSize());
      }
      if (!ramBufferSize.isNull()) {
         props.put(RAM_BUFFER_KEY, ramBufferSize());
      }
      if (!maxBufferedDocs.isNull()) {
         props.put(MAX_BUFFER_DOCS_KEY, maxBufferedDocs());
      }
      if (lowLevelTrace.isModified()) {
         props.put(LOW_LEVEL_TRACE_KEY, isLowLevelTrace());
      }
      props.putAll(indexMergeConfigurationBuilder.asInternalProperties());
      return props;
   }



   public IndexMergeConfigurationBuilder merge() {
      return indexMergeConfigurationBuilder;
   }

   public int threadPoolSize() {
      return threadPoolSize.get();
   }

   public IndexWriterConfigurationBuilder threadPoolSize(int value) {
      threadPoolSize.set(value);
      return this;
   }

   public int queueCount() {
      return queueCount.get();
   }

   public IndexWriterConfigurationBuilder queueCount(int value) {
      queueCount.set(value);
      return this;
   }

   public int queueSize() {
      return queueSize.get();
   }

   public IndexWriterConfigurationBuilder queueSize(int value) {
      queueSize.set(value);
      return this;
   }

   public int commitInterval() {
      return commitInterval.get();
   }

   public IndexWriterConfigurationBuilder commitInterval(int value) {
      commitInterval.set(value);
      return this;
   }

   public int ramBufferSize() {
      return ramBufferSize.get();
   }

   public IndexWriterConfigurationBuilder ramBufferSize(int value) {
      ramBufferSize.set(value);
      return this;
   }

   public int maxBufferedDocs() {
      return maxBufferedDocs.get();
   }

   public IndexWriterConfigurationBuilder maxBufferedDocs(int value) {
      maxBufferedDocs.set(value);
      return this;
   }

   public boolean isLowLevelTrace() {
      return lowLevelTrace.get();
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
