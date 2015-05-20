package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work when rehash is disabled on a parallel stream with parallel distribution
 */
@Test(groups = "functional", testName = "streams.DistributedParallelStreamTest")
public class DistributedParallelNonRehashStreamTest extends DistributedStreamTest {
   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.parallelStream().disableRehashAware().parallelDistribution();
   }
}
