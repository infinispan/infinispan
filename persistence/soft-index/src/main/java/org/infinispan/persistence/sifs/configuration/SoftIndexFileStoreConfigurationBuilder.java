package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.*;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SoftIndexFileStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<SoftIndexFileStoreConfiguration, SoftIndexFileStoreConfigurationBuilder> {

   public SoftIndexFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, SoftIndexFileStoreConfiguration.attributeDefinitionSet());
   }

   public SoftIndexFileStoreConfigurationBuilder dataLocation(String dataLocation) {
      attributes.attribute(DATA_LOCATION).set(dataLocation);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexLocation(String indexLocation) {
      attributes.attribute(INDEX_LOCATION).set(indexLocation);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexSegments(int indexSegments) {
      attributes.attribute(INDEX_SEGMENTS).set(indexSegments);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder maxFileSize(int maxFileSize) {
      attributes.attribute(MAX_FILE_SIZE).set(maxFileSize);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder minNodeSize(int minNodeSize) {
      attributes.attribute(MIN_NODE_SIZE).set(minNodeSize);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder maxNodeSize(int maxNodeSize) {
      attributes.attribute(MAX_NODE_SIZE).set(maxNodeSize);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexQueueLength(int indexQueueLength) {
      attributes.attribute(INDEX_QUEUE_LENGTH).set(indexQueueLength);
      return this;
   }
   public SoftIndexFileStoreConfigurationBuilder syncWrites(boolean syncWrites) {
      attributes.attribute(SYNC_WRITES).set(syncWrites);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder openFilesLimit(int openFilesLimit) {
      attributes.attribute(OPEN_FILES_LIMIT).set(openFilesLimit);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder compactionThreshold(double compactionThreshold) {
      attributes.attribute(COMPACTION_THRESHOLD).set(compactionThreshold);
      return this;
   }

   @Override
   public SoftIndexFileStoreConfiguration create() {
      return new SoftIndexFileStoreConfiguration(attributes.protect(),
            async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(SoftIndexFileStoreConfiguration template) {
      super.read(template);
      return this;
   }

   @Override
   public SoftIndexFileStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public String toString() {
      return "SoftIndexFileStoreConfigurationBuilder [attributes=" + attributes + ", async=" + async + ", singletonStore=" + singletonStore + "]";
   }
}
