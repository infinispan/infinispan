package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SoftIndexFileStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<SoftIndexFileStoreConfiguration, SoftIndexFileStoreConfigurationBuilder> {

   private String dataLocation = "Infinispan-SoftIndexFileStore-Data";
   private String indexLocation = "Infinispan-SoftIndexFileStore-Index";
   private int indexSegments = 3;
   private int maxFileSize = 16 * 1024 * 1024;
   private int minNodeSize = -1;
   private int maxNodeSize = 4096;
   private int indexQueueLength = 1000;
   private boolean syncWrites = false;
   private int openFilesLimit = 1000;
   private double compactionThreshold = 0.5;

   public SoftIndexFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   public SoftIndexFileStoreConfigurationBuilder dataLocation(String dataLocation) {
      this.dataLocation = dataLocation;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexLocation(String indexLocation) {
      this.indexLocation = indexLocation;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexSegments(int indexSegments) {
      this.indexSegments = indexSegments;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder maxFileSize(int maxFileSize) {
      this.maxFileSize = maxFileSize;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder minNodeSize(int minNodeSize) {
      this.minNodeSize = minNodeSize;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder maxNodeSize(int maxNodeSize) {
      this.maxNodeSize = maxNodeSize;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexQueueLength(int indexQueueLength) {
      this.indexQueueLength = indexQueueLength;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder syncWrites(boolean syncWrites) {
      this.syncWrites = syncWrites;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder openFilesLimit(int openFilesLimit) {
      this.openFilesLimit = openFilesLimit;
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder compactionThreshold(double compactionThreshold) {
      this.compactionThreshold = compactionThreshold;
      return this;
   }

   @Override
   public SoftIndexFileStoreConfiguration create() {
      return new SoftIndexFileStoreConfiguration(
            purgeOnStartup, fetchPersistentState, ignoreModifications,
            async.create(), singletonStore.create(), preload, shared, properties,
            dataLocation, indexLocation, indexSegments,
            maxFileSize, minNodeSize < 0 ? maxNodeSize/3 : minNodeSize, maxNodeSize,
            indexQueueLength, syncWrites, openFilesLimit, compactionThreshold);
   }

   @Override
   public Builder<?> read(SoftIndexFileStoreConfiguration template) {
      dataLocation = template.dataLocation();
      indexLocation = template.indexLocation();
      indexSegments = template.indexSegments();
      maxFileSize = template.maxFileSize();
      minNodeSize = template.minNodeSize();
      maxNodeSize = template.maxNodeSize();
      indexQueueLength = template.indexQueueLength();
      syncWrites = template.syncWrites();
      openFilesLimit = template.openFilesLimit();
      compactionThreshold = template.compactionThreshold();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      preload = template.preload();
      shared = template.shared();

      return this;
   }

   @Override
   public SoftIndexFileStoreConfigurationBuilder self() {
      return this;
   }
}
