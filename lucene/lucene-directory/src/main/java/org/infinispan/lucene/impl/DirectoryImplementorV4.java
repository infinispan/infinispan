package org.infinispan.lucene.impl;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

/**
 * Extension of org.infinispan.lucene.impl.DirectoryImplementor.DirectoryImplementor to allow
 * returning a Lucene v4 compatible implementation of org.apache.lucene.store.IndexOutput. This is
 * needed to implement the new getChecksum() method added in Lucene v.4.8.
 *
 * @since 7.0
 * @author Sanne Grinovero
 */
public class DirectoryImplementorV4 extends org.infinispan.lucene.impl.DirectoryImplementor {

   private final FileCacheKey segmentsGenFileKey;

   /**
    * Simple delegation constructor
    */
   public DirectoryImplementorV4(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, String indexName, int chunkSize, SegmentReadLocker readLocker) {
      super(metadataCache, chunksCache, indexName, chunkSize, readLocker);
      segmentsGenFileKey = new FileCacheKey(indexName, IndexFileNames.SEGMENTS_GEN);
   }

   IndexOutput createOutput(final String name) {
      if (IndexFileNames.SEGMENTS_GEN.equals(name)) {
         return new CheckSummingIndexOutput(metadataCache, chunksCache, segmentsGenFileKey, chunkSize, fileOps);
      }
      else {
         final FileCacheKey key = new FileCacheKey(indexName, name);
         // creating new file, metadata is added on flush() or close() of
         // IndexOutPut
         return new CheckSummingIndexOutput(metadataCache, chunksCache, key, chunkSize, fileOps);
      }
   }

}
