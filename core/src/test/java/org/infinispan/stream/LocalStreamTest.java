package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work on a local stream
 */
@Test(groups = "functional", testName = "streams.LocalStreamTest")
public class LocalStreamTest extends BaseStreamTest {
   public LocalStreamTest() {
      super(false);
      cacheMode(CacheMode.LOCAL);
   }

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.stream();
   }

   @Test(enabled = false)
   @Override
   public void testKeySegmentFilter() {

   }
}
