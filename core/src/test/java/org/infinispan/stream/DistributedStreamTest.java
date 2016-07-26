package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work on a regular distrbuted stream
 */
@Test(groups = "functional", testName = "streams.DistributedStreamTest")
public class DistributedStreamTest extends BaseStreamTest {

   public DistributedStreamTest() {
      super(false);
      cacheMode(CacheMode.DIST_SYNC);
   }

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      // This forces parallel distribution since iterator defaults to sequential
      return entries.stream().parallelDistribution();
   }
}
