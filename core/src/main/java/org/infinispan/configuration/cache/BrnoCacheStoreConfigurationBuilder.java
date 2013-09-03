package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class BrnoCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<BrnoCacheStoreConfiguration, BrnoCacheStoreConfigurationBuilder>{

   private String dataLocation = "Infinispan-BrnoCacheStore-Data";
   private String indexLocation = "Infinispan-BrnoCacheStore-Index";
   private int indexSegments = 3;
   private int maxFileSize = 16 * 1024 * 1024;
   private int minNodeSize = -1;
   private int maxNodeSize = 4096;
   private int indexQueueLength = 1000;
   private boolean syncWrites = false;
   private int openFilesLimit = 1000;
   private double compactionThreshold = 0.5;

   public BrnoCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   public BrnoCacheStoreConfigurationBuilder dataLocation(String dataLocation) {
      this.dataLocation = dataLocation;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder indexLocation(String indexLocation) {
      this.indexLocation = indexLocation;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder indexSegments(int indexSegments) {
      this.indexSegments = indexSegments;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder maxFileSize(int maxFileSize) {
      this.maxFileSize = maxFileSize;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder minNodeSize(int minNodeSize) {
      this.minNodeSize = minNodeSize;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder maxNodeSize(int maxNodeSize) {
      this.maxNodeSize = maxNodeSize;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder indexQueueLength(int indexQueueLength) {
      this.indexQueueLength = indexQueueLength;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder syncWrites(boolean syncWrites) {
      this.syncWrites = syncWrites;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder openFilesLimit(int openFilesLimit) {
      this.openFilesLimit = openFilesLimit;
      return this;
   }

   public BrnoCacheStoreConfigurationBuilder compactionThreshold(double compactionThreshold) {
      this.compactionThreshold = compactionThreshold;
      return this;
   }

   @Override
   public BrnoCacheStoreConfiguration create() {
      return new BrnoCacheStoreConfiguration(dataLocation, indexLocation, indexSegments,
            maxFileSize, minNodeSize < 0 ? maxNodeSize/3 : minNodeSize, maxNodeSize,
            indexQueueLength, syncWrites, openFilesLimit, compactionThreshold,
            purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(BrnoCacheStoreConfiguration template) {
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
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }

   @Override
   public BrnoCacheStoreConfigurationBuilder self() {
      return this;
   }
}
