package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.COMPACTION_THRESHOLD;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.DATA_LOCATION;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.INDEX_LOCATION;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.INDEX_QUEUE_LENGTH;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.INDEX_SEGMENTS;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.MAX_FILE_SIZE;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.MAX_NODE_SIZE;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.MIN_NODE_SIZE;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.OPEN_FILES_LIMIT;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.SYNC_WRITES;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.sifs.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SoftIndexFileStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<SoftIndexFileStoreConfiguration, SoftIndexFileStoreConfigurationBuilder> {
   private static final Log log = LogFactory.getLog(SoftIndexFileStoreConfigurationBuilder.class, Log.class);

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
   protected void validate(boolean skipClassChecks) {
      super.validate(skipClassChecks);
      int minNodeSize = attributes.attribute(MIN_NODE_SIZE).get();
      int maxNodeSize = attributes.attribute(MAX_NODE_SIZE).get();
      if (maxNodeSize <= 0 || maxNodeSize > Short.MAX_VALUE) {
         throw log.maxNodeSizeLimitedToShort(maxNodeSize);
      } else if (minNodeSize < 0 || minNodeSize > maxNodeSize) {
         throw log.minNodeSizeMustBeLessOrEqualToMax(minNodeSize, maxNodeSize);
      }
      double compactionThreshold = attributes.attribute(COMPACTION_THRESHOLD).get();
      if (compactionThreshold <= 0 || compactionThreshold > 1) {
         throw log.invalidCompactionThreshold(compactionThreshold);
      }
   }

   @Override
   public String toString() {
      return "SoftIndexFileStoreConfigurationBuilder [attributes=" + attributes + ", async=" + async + ", singletonStore=" + singletonStore + "]";
   }
}
