package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work when stream is parallel with parallel distribution
 */
@Test(groups = "functional", testName = "streams.DistributedParallelStreamTest")
@InCacheMode({ CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC })
public class DistributedParallelStreamTest extends DistributedStreamTest {

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.parallelStream().parallelDistribution();
   }
}
