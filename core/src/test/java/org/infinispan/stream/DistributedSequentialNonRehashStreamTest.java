package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work when rehash is disabled on a sequential stream
 */
@Test(groups = "functional", testName = "streams.DistributedSequentialNonRehashStreamTest")
@InCacheMode({ CacheMode.DIST_SYNC })
public class DistributedSequentialNonRehashStreamTest extends DistributedStreamTest {

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.stream().sequentialDistribution().disableRehashAware();
   }
}
