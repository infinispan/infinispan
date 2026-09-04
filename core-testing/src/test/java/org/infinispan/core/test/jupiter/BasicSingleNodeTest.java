package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.core.test.jupiter.InfinispanExtension.k;
import static org.infinispan.core.test.jupiter.InfinispanExtension.v;

import org.infinispan.Cache;
import org.junit.jupiter.api.Test;

/**
 * Validates the test harness with a single non-clustered node.
 */
@InfinispanCluster
class BasicSingleNodeTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   void testPutAndGet() {
      InfinispanContext.CacheHandle<String, String> handle = ctx.createCache();
      Cache<String, String> cache = handle.cache();

      cache.put(k(), v());
      assertThat(cache.get(k())).isEqualTo(v());
   }

   @Test
   void testCacheIsolation() {
      InfinispanContext.CacheHandle<String, String> cache1 = ctx.createCache();
      InfinispanContext.CacheHandle<String, String> cache2 = ctx.createCache();

      cache1.cache().put(k(), v(1));
      cache2.cache().put(k(), v(2));

      assertThat(cache1.cache().get(k())).isEqualTo(v(1));
      assertThat(cache2.cache().get(k())).isEqualTo(v(2));
   }

   @Test
   void testSingleNode() {
      assertThat(ctx.numNodes()).isEqualTo(1);
   }
}
