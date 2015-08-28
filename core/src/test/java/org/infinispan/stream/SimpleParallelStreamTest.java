package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "streams.LocalStreamTest")
public class SimpleParallelStreamTest extends BaseStreamTest {
   public SimpleParallelStreamTest() {
      super(false, CacheMode.LOCAL);
   }

   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      builder.simpleCache(true);
   }

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> cacheCollection) {
      return cacheCollection.parallelStream();
   }
}
