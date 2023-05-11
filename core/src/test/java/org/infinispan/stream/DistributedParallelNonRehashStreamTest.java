package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work when rehash is disabled on a parallel stream with parallel distribution
 */
@Test(groups = "functional", testName = "streams.DistributedParallelNonRehashStreamTest")
@InCacheMode({ CacheMode.DIST_SYNC })
public class DistributedParallelNonRehashStreamTest extends DistributedStreamTest {

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.parallelStream().disableRehashAware().parallelDistribution();
   }
}
