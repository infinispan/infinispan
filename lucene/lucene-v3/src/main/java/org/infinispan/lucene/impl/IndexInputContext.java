package org.infinispan.lucene.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

public final class IndexInputContext {

   final AdvancedCache<ChunkCacheKey, Object> chunksCache;
   final FileCacheKey fileKey;
   final FileMetadata fileMetadata;
   final SegmentReadLocker readLocks;

   public IndexInputContext(AdvancedCache<ChunkCacheKey, Object> chunksCache, FileCacheKey fileKey, FileMetadata fileMetadata,
         SegmentReadLocker readLocks) {
            this.chunksCache = chunksCache;
            this.fileKey = fileKey;
            this.fileMetadata = fileMetadata;
            this.readLocks = readLocks;
   }

}
