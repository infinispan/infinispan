package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work on a local parallel stream
 */
@Test(groups = "functional", testName = "streams.LocalParallelStreamTest")
public class LocalParallelStreamTest extends BaseStreamTest {
   public LocalParallelStreamTest() {
      super(false, CacheMode.LOCAL);
   }

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.parallelStream();
   }
}
