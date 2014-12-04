package org.infinispan.lucene.testutils;

import org.infinispan.Cache;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;

/**
 * Same as {@link org.infinispan.lucene.readlocks.DistributedSegmentReadLocker}, but force deletes to be sync
 *
 * @author gustavonalle
 * @since 7.1
 */
public class TestSegmentReadLocker extends DistributedSegmentReadLocker {
   public TestSegmentReadLocker(Cache<Object, Integer> locksCache, Cache<?, ?> chunksCache, Cache<?, ?> metadataCache, String indexName) {
      super(locksCache, chunksCache, metadataCache, indexName, true);
   }
}
