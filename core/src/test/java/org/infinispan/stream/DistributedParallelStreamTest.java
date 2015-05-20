package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work when stream is parallel with parallel distribution
 */
@Test(groups = "functional", testName = "streams.DistributedParallelStreamTest")
public class DistributedParallelStreamTest extends DistributedStreamTest {
   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.parallelStream().parallelDistribution();
   }
}
