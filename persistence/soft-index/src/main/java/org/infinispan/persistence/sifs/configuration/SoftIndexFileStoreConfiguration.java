package org.infinispan.persistence.sifs.configuration;

import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
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
   private final String dataLocation;
   private final String indexLocation;
   private final int indexSegments;
   private final int maxFileSize;
   private final int minNodeSize;
   private final int maxNodeSize;
   private final int indexQueueLength;
   private final boolean syncWrites;
   private final int openFilesLimit;
   private final double compactionThreshold;


   public SoftIndexFileStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
                                          boolean ignoreModifications, AsyncStoreConfiguration async,
                                          SingletonStoreConfiguration singletonStore, boolean preload, boolean shared,
                                          Properties properties,
                                          String dataLocation, String indexLocation, int indexSegments,
                                          int maxFileSize, int minNodeSize, int maxNodeSize, int indexQueueLength,
                                          boolean syncWrites, int openFilesLimit, double compactionThreshold) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.dataLocation = dataLocation;
      this.indexLocation = indexLocation;
      this.indexSegments = indexSegments;
      this.minNodeSize = minNodeSize;
      this.maxFileSize = maxFileSize;
      this.maxNodeSize = maxNodeSize;
      this.indexQueueLength = indexQueueLength;
      this.syncWrites = syncWrites;
      this.openFilesLimit = openFilesLimit;
      this.compactionThreshold = compactionThreshold;
   }

   public String dataLocation() {
      return dataLocation;
   }

   public String indexLocation() {
      return indexLocation;
   }

   public int indexSegments() {
      return indexSegments;
   }

   public int maxFileSize() {
      return maxFileSize;
   }

   public int minNodeSize() {
      return minNodeSize;
   }

   public int maxNodeSize() {
      return maxNodeSize;
   }

   public int indexQueueLength() {
      return indexQueueLength;
   }

   public boolean syncWrites() {
      return syncWrites;
   }

   public int openFilesLimit() {
      return openFilesLimit;
   }

   public double compactionThreshold() {
      return compactionThreshold;
   }



}
