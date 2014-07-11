package org.infinispan.lucene.directory;

import org.infinispan.Cache;
import org.infinispan.lucene.impl.DirectoryBuilderImpl;

/**
 * Builder class to create instances of the {@link org.apache.lucene.store.Directory} implementation which stored data
 * in the data grid.
 */
public final class DirectoryBuilder {

    private DirectoryBuilder() {
        //not to be created
    }

    /**
     * Starting point to create a Directory instance.
     *
     * @param metadataCache contains the metadata of stored elements
     * @param chunksCache cache containing the bulk of the index; this is the larger part of data
     * @param distLocksCache cache to store locks; should be replicated and not using a persistent CacheStore
     * @param indexName identifies the index; you can store different indexes in the same set of caches using different identifiers
     */
    public static BuildContext newDirectoryInstance(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
        return new DirectoryBuilderImpl(metadataCache, chunksCache, distLocksCache, indexName);
    }

}
