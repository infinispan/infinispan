package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.core.test.jupiter.InfinispanExtension.k;
import static org.infinispan.core.test.jupiter.InfinispanExtension.v;

import org.infinispan.configuration.cache.CacheMode;
import org.junit.jupiter.api.Test;

/**
 * Validates clustered cache operations with the test harness.
 */
@InfinispanCluster(numNodes = 2)
class ClusteredCacheTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   void testReplicatedCache() {
      var cache = ctx.<String, String>createCache(b ->
            b.clustering().cacheMode(CacheMode.REPL_SYNC));

      cache.on(0).put(k(), v());
      assertThat(cache.on(1).get(k())).isEqualTo(v());
   }

   @Test
   void testDistributedCache() {
      var cache = ctx.<String, String>createCache(b ->
            b.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1));

      for (int i = 0; i < 100; i++) {
         cache.on(0).put(k(i), v(i));
      }

      // All entries should be accessible from either node
      for (int i = 0; i < 100; i++) {
         String v0 = cache.on(0).get(k(i));
         String v1 = cache.on(1).get(k(i));
         assertThat(v0).isEqualTo(v(i));
         assertThat(v1).isEqualTo(v(i));
      }
   }

   @Test
   void testClusterSize() {
      assertThat(ctx.numNodes()).isEqualTo(2);
      assertThat(ctx.manager(0).getMembers()).hasSize(2);
      assertThat(ctx.manager(1).getMembers()).hasSize(2);
   }
}
