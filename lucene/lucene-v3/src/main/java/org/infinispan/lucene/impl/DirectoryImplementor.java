package org.infinispan.lucene.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.lucene.store.IndexOutput;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.readlocks.SegmentReadLocker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Common code for different Directory implementations. Extracted to accommodate support
 * for both Apache Lucene v.3.x and v.4
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
final class DirectoryImplementor {

    private static final Log log = LogFactory.getLog(DirectoryImplementor.class);

    private final AdvancedCache<FileCacheKey, FileMetadata> metadataCache;
    private final AdvancedCache<ChunkCacheKey, Object> chunksCache;

    // indexName is used to be able to store multiple named indexes in the same caches
    private final String indexName;

    // chunk size used for this Directory
    private final int chunkSize;

    private final FileListOperations fileOps;
    private final SegmentReadLocker readLocks;

    public DirectoryImplementor(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, String indexName, int chunkSize, SegmentReadLocker readLocker) {
        if (chunkSize <= 0)
           throw new IllegalArgumentException("chunkSize must be a positive integer");
        this.metadataCache = (AdvancedCache<FileCacheKey, FileMetadata>) metadataCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
        this.chunksCache = (AdvancedCache<ChunkCacheKey, Object>) chunksCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
        this.indexName = indexName;
        this.chunkSize = chunkSize;
        this.fileOps = new FileListOperations(this.metadataCache, indexName);
        this.readLocks = readLocker;
     }

    String[] list() {
       final Set<String> files = fileOps.getFileList();
       //Careful! if you think you can optimize this array allocation, think again.
       //The _files_ are a concurrent structure, its size could vary in parallel:
       //the array population and dimensioning need to be performed atomically
       //to avoid trailing null elements in the returned array.
       final String[] array = files.toArray(new String[0]);
       return array;
    }

    boolean fileExists(final String name) {
       return fileOps.getFileList().contains(name);
    }

    /**
     * Used by Lucene v3.x only
     */
    long fileModified(final String name) {
       final FileMetadata fileMetadata = fileOps.getFileMetadata(name);
       if (fileMetadata == null) {
          return 0L;
       }
       else {
          return fileMetadata.getLastModified();
       }
    }

    /**
     * Used by Lucene v3.x only
     */
    void touchFile(final String fileName) {
       final FileMetadata file = fileOps.getFileMetadata(fileName);
       if (file != null) {
          final FileCacheKey key = new FileCacheKey(indexName, fileName);
          file.touch();
          metadataCache.put(key, file);
       }
    }

    void deleteFile(final String name) {
       fileOps.deleteFileName(name);
       readLocks.deleteOrReleaseReadLock(name);
       if (log.isDebugEnabled()) {
          log.debugf("Removed file: %s from index: %s", name, indexName);
       }
    }

    void renameFile(final String from, final String to) {
       final FileCacheKey fromKey = new FileCacheKey(indexName, from);
       final FileMetadata metadata = metadataCache.get(fromKey);
       final int bufferSize = metadata.getBufferSize();
       // preparation: copy all chunks to new keys
       int i = -1;
       Object ob;
       do {
          final ChunkCacheKey fromChunkKey = new ChunkCacheKey(indexName, from, ++i, bufferSize);
          ob = chunksCache.get(fromChunkKey);
          if (ob == null) {
             break;
          }
          final ChunkCacheKey toChunkKey = new ChunkCacheKey(indexName, to, i, bufferSize);
          chunksCache.withFlags(Flag.IGNORE_RETURN_VALUES).put(toChunkKey, ob);
       } while (true);

       // rename metadata first

       metadataCache.put(new FileCacheKey(indexName, to), metadata);
       fileOps.removeAndAdd(from, to);

       // now trigger deletion of old file chunks:
       readLocks.deleteOrReleaseReadLock(from);
       if (log.isTraceEnabled()) {
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
       final FileCacheKey key = new FileCacheKey(indexName, name);
       // creating new file, metadata is added on flush() or close() of IndexOutPut
       return new InfinispanIndexOutput(metadataCache, chunksCache, key, chunkSize, fileOps);
    }

    IndexInputContext openInput(final String name) throws IOException {
       final FileCacheKey fileKey = new FileCacheKey(indexName, name);
       final FileMetadata fileMetadata = metadataCache.get(fileKey);
       if (fileMetadata == null) {
          throw new FileNotFoundException("Error loading metadata for index file: " + fileKey);
       }
       else if (fileMetadata.getSize() <= fileMetadata.getBufferSize()) {
          //files smaller than chunkSize don't need a readLock
          return new IndexInputContext(chunksCache, fileKey, fileMetadata, null);
       }
       else {
          boolean locked = readLocks.acquireReadLock(name);
          if (!locked) {
             // safest reaction is to tell this file doesn't exist anymore.
             throw new FileNotFoundException("Error loading metadata for index file: " + fileKey);
          }
          return new IndexInputContext(chunksCache, fileKey, fileMetadata, readLocks);
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

}
