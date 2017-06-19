package org.infinispan.lucene.directory;

import java.util.Arrays;
import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.lucene.impl.DirectoryBuilderImpl;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Builder class to create instances of the {@link org.apache.lucene.store.Directory} implementation which stored data
 * in the data grid.
 */
public final class DirectoryBuilder {

   private static final Log log = LogFactory.getLog(DirectoryBuilder.class, Log.class);


   private DirectoryBuilder() {
      //not to be created
   }

   /**
    * Starting point to create a Directory instance.
    *
    * @param metadataCache  contains the metadata of stored elements
    * @param chunksCache    cache containing the bulk of the index; this is the larger part of data
    * @param distLocksCache cache to store locks; should be replicated and not using a persistent CacheStore
    * @param indexName      identifies the index; you can store different indexes in the same set of caches using
    *                       different identifiers
    */
   public static BuildContext newDirectoryInstance(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
      validateIndexCaches(indexName, metadataCache, chunksCache, distLocksCache);
      return new DirectoryBuilderImpl(metadataCache, chunksCache, distLocksCache, indexName);
   }

   private static void validateIndexCaches(String indexName, Cache<?, ?>... caches) {
      Arrays.stream(caches).filter(Objects::nonNull).forEach(cache -> {
         ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
         CacheMode cacheMode = clusteringConfiguration.cacheMode();
         if (cacheMode.isClustered() && !cacheMode.isSynchronous()) {
            throw log.cannotStoreIndexOnAsyncCaches(indexName, cache.getName(), cacheMode);
         }
      });
   }

}
