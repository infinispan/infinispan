package org.infinispan.lucene.testutils;

import org.infinispan.Cache;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;

/**
 * Same as {@link org.infinispan.lucene.readlocks.DistributedSegmentReadLocker}, but force deletes to be sync
 *
 * @author gustavonalle
 * @since 7.1
 */
@SuppressWarnings("unchecked")
public class TestSegmentReadLocker extends DistributedSegmentReadLocker {
   public TestSegmentReadLocker(Cache<?, ?> locksCache, Cache<?, ?> chunksCache, Cache<?, ?> metadataCache, String indexName) {
      super((Cache<Object, Integer>) locksCache, chunksCache, metadataCache, indexName, -1, true);
   }
}
