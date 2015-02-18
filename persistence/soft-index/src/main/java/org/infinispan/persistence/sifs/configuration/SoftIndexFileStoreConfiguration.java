package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.sifs.SoftIndexFileStore;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@BuiltBy(SoftIndexFileStoreConfigurationBuilder.class)
@ConfigurationFor(SoftIndexFileStore.class)
public class SoftIndexFileStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<String> DATA_LOCATION = AttributeDefinition.builder("dataLocation", "Infinispan-SoftIndexFileStore-Data").immutable().build();
   static final AttributeDefinition<String> INDEX_LOCATION = AttributeDefinition.builder("indexLocation", "Infinispan-SoftIndexFileStore-Index").immutable().build();
   static final AttributeDefinition<Integer> INDEX_SEGMENTS = AttributeDefinition.builder("indexSegments", 3).immutable().build();
   static final AttributeDefinition<Integer> MAX_FILE_SIZE = AttributeDefinition.builder("maxFileSize", 16 * 1024 * 1024).immutable().build();
   static final AttributeDefinition<Integer> MIN_NODE_SIZE = AttributeDefinition.builder("minNodeSize", -1).immutable().build();
   static final AttributeDefinition<Integer> MAX_NODE_SIZE = AttributeDefinition.builder("maxNodeSize", 4096).immutable().build();
   static final AttributeDefinition<Integer> INDEX_QUEUE_LENGTH = AttributeDefinition.builder("indexQueueLength", 1000).immutable().build();
   static final AttributeDefinition<Boolean> SYNC_WRITES = AttributeDefinition.builder("syncWrites", false).immutable().build();
   static final AttributeDefinition<Integer> OPEN_FILES_LIMIT = AttributeDefinition.builder("openFilesLimit", 1000).immutable().build();
   static final AttributeDefinition<Double> COMPACTION_THRESHOLD = AttributeDefinition.builder("compactionThreshold", 0.5d).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(SoftIndexFileStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), DATA_LOCATION, INDEX_LOCATION, INDEX_SEGMENTS, MAX_FILE_SIZE,
            MIN_NODE_SIZE, MAX_NODE_SIZE, INDEX_QUEUE_LENGTH, SYNC_WRITES, OPEN_FILES_LIMIT, COMPACTION_THRESHOLD);
   }

   public SoftIndexFileStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public String dataLocation() {
      return attributes.attribute(DATA_LOCATION).asString();
   }

   public String indexLocation() {
      return attributes.attribute(INDEX_LOCATION).asString();
   }

   public int indexSegments() {
      return attributes.attribute(INDEX_SEGMENTS).asInteger();
   }

   public int maxFileSize() {
      return attributes.attribute(MAX_FILE_SIZE).asInteger();
   }

   public int minNodeSize() {
      return attributes.attribute(MIN_NODE_SIZE).asInteger();
   }

   public int maxNodeSize() {
      return attributes.attribute(MAX_NODE_SIZE).asInteger();
   }

   public int indexQueueLength() {
      return attributes.attribute(INDEX_QUEUE_LENGTH).asInteger();
   }

   public boolean syncWrites() {
      return attributes.attribute(SYNC_WRITES).asBoolean();
   }

   public int openFilesLimit() {
      return attributes.attribute(OPEN_FILES_LIMIT).asInteger();
   }

   public double compactionThreshold() {
      return attributes.attribute(COMPACTION_THRESHOLD).asDouble();
   }

}
