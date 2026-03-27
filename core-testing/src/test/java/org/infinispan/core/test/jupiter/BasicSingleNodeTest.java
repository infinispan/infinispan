package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

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

      cache.put("key", "value");
      assertThat(cache.get("key")).isEqualTo("value");
   }

   @Test
   void testCacheIsolation() {
      InfinispanContext.CacheHandle<String, String> cache1 = ctx.createCache();
      InfinispanContext.CacheHandle<String, String> cache2 = ctx.createCache();

      cache1.cache().put("key", "v1");
      cache2.cache().put("key", "v2");

      assertThat(cache1.cache().get("key")).isEqualTo("v1");
      assertThat(cache2.cache().get("key")).isEqualTo("v2");
   }

   @Test
   void testSingleNode() {
      assertThat(ctx.numNodes()).isEqualTo(1);
   }
}
