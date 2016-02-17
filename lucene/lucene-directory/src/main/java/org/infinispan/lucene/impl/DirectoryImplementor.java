package org.infinispan.lucene.impl;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Common code for different Directory implementations.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
class DirectoryImplementor {

    private static final Log log = LogFactory.getLog(DirectoryImplementor.class);
    private static final boolean trace = log.isTraceEnabled();

    protected final AdvancedCache<FileCacheKey, FileMetadata> metadataCache;
    protected final AdvancedCache<ChunkCacheKey, Object> chunksCache;
    protected final AdvancedCache<Object, Integer> distLocksCache;

    // indexName is used to be able to store multiple named indexes in the same caches
    protected final String indexName;

    // chunk size used for this Directory
    protected final int chunkSize;

    protected final FileListOperations fileOps;
    private final SegmentReadLocker readLocks;
    private final FileCacheKey segmentsGenFileKey;
    private final int affinitySegmentId;

    public DirectoryImplementor(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName, int chunkSize, SegmentReadLocker readLocker, boolean fileListUpdatedAsync, int affinitySegmentId) {
       this.affinitySegmentId = affinitySegmentId;
       if (chunkSize <= 0)
           throw new IllegalArgumentException("chunkSize must be a positive integer");
        this.metadataCache = (AdvancedCache<FileCacheKey, FileMetadata>) metadataCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
        this.chunksCache = (AdvancedCache<ChunkCacheKey, Object>) chunksCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
        this.distLocksCache = (AdvancedCache<Object, Integer>) distLocksCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
        this.indexName = indexName;
        this.chunkSize = chunkSize;
        this.fileOps = new FileListOperations(this.metadataCache, indexName, fileListUpdatedAsync, this.affinitySegmentId);
        this.segmentsGenFileKey = new FileCacheKey(indexName, IndexFileNames.SEGMENTS, this.affinitySegmentId);
        this.readLocks = readLocker;
     }

    String[] list() {
       return fileOps.listFilenames();
    }

    boolean fileExists(final String name) {
       return fileOps.fileExists(name);
    }

    void deleteFile(final String name) {
       fileOps.deleteFileName(name);
       readLocks.deleteOrReleaseReadLock(name);
       if (log.isDebugEnabled()) {
          log.debugf("Removed file: %s from index: %s", name, indexName);
       }
    }

    void renameFile(final String from, final String to) {
       final FileCacheKey fromKey = new FileCacheKey(indexName, from, affinitySegmentId);
       final FileMetadata metadata = metadataCache.get(fromKey);
       final int bufferSize = metadata.getBufferSize();
       // preparation: copy all chunks to new keys
       int i = -1;
       Object ob;
       do {
          final ChunkCacheKey fromChunkKey = new ChunkCacheKey(indexName, from, ++i, bufferSize, affinitySegmentId);
          ob = chunksCache.get(fromChunkKey);
          if (ob == null) {
             break;
          }
          final ChunkCacheKey toChunkKey = new ChunkCacheKey(indexName, to, i, bufferSize, affinitySegmentId);
          chunksCache.withFlags(Flag.IGNORE_RETURN_VALUES).put(toChunkKey, ob);
       } while (true);

       // rename metadata first

       metadataCache.put(new FileCacheKey(indexName, to, affinitySegmentId), metadata);
       fileOps.removeAndAdd(from, to);

       // now trigger deletion of old file chunks:
       readLocks.deleteOrReleaseReadLock(from);
       if (trace) {
          log.tracef("Renamed file from: %s to: %s in index %s", from, to, indexName);
       }
    }

    long fileLength(final String name) {
       final FileMetadata fileMetadata = fileOps.getFileMetadata(name);
       if (fileMetadata == null) {
          return 0L; //as in FSDirectory (RAMDirectory throws an exception instead)
       }
       else {
          return fileMetadata.getSize();
       }
    }

    IndexOutput createOutput(final String name) {
       if (IndexFileNames.SEGMENTS.equals(name)) {
          return new InfinispanIndexOutput(metadataCache, chunksCache, segmentsGenFileKey, chunkSize, fileOps, affinitySegmentId);
       }
       else {
          final FileCacheKey key = new FileCacheKey(indexName, name, affinitySegmentId);
          // creating new file, metadata is added on flush() or close() of
          // IndexOutPut
          return new InfinispanIndexOutput(metadataCache, chunksCache, key, chunkSize, fileOps, affinitySegmentId);
       }
    }

    IndexInputContext openInput(final String name) throws IOException {
       final FileCacheKey fileKey = new FileCacheKey(indexName, name, affinitySegmentId);
       FileMetadata fileMetadata;
       try {
          fileMetadata = metadataCache.get(fileKey);
       }
       catch (PersistenceException pe) {
          //When loading through the LuceneCacheLoader, a valid FileNotFoundException would be wrapped by a PersistenceException:
          //just ignore it so that we re-throw the needed FileNotFoundException
          fileMetadata = null;
       }
       if (fileMetadata == null) {
          throw new FileNotFoundException("Error loading metadata for index file: " + fileKey);
       }
       else if (!fileMetadata.isMultiChunked()) {
          //files smaller than chunkSize don't need a readLock
          return new IndexInputContext(chunksCache, fileKey, fileMetadata, null, affinitySegmentId);
       }
       else {
          boolean locked = readLocks.acquireReadLock(name);
          if (!locked) {
             // safest reaction is to tell this file doesn't exist anymore.
             throw new FileNotFoundException("Error loading metadata for index file: " + fileKey);
          }
          return new IndexInputContext(chunksCache, fileKey, fileMetadata, readLocks, affinitySegmentId);
       }
    }

    /**
     * @return The value of indexName, same constant as provided to the constructor.
     */
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String toString() {
       return "DirectoryImplementor{indexName=\'" + indexName + "\'}";
    }

    public int getChunkSize() {
       return chunkSize;
    }

    public Cache getMetadataCache() {
       return metadataCache;
    }

    public Cache getDataCache() {
       return chunksCache;
    }

    public Cache<Object, Integer> getDistLocksCache() {
      return distLocksCache;
    }

}
