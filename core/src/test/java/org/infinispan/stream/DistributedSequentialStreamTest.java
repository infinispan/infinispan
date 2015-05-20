package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work on a sequential stream
 */
@Test(groups = "functional", testName = "streams.DistributedSequentialStreamTest")
public class DistributedSequentialStreamTest extends DistributedStreamTest {
   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.stream().sequentialDistribution();
   }
}
