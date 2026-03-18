package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.core.test.jupiter.InfinispanExtension.k;
import static org.infinispan.core.test.jupiter.InfinispanExtension.v;

import org.infinispan.configuration.cache.CacheMode;
import org.junit.jupiter.api.Test;

/**
 * Validates creating caches from declarative XML configuration strings and files.
 */
@InfinispanCluster(numNodes = 2)
class DeclarativeCacheTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   void testCreateFromXmlString() {
      var cache = ctx.<String, String>createCacheFromString("""
            <distributed-cache name="ignored" mode="SYNC" owners="2">
               <memory max-count="200"/>
            </distributed-cache>
            """);

      cache.on(0).put(k(), v());
      assertThat(cache.on(1).get(k())).isEqualTo(v());

      // Verify the configuration was applied
      var config = ctx.manager(0).getCacheConfiguration(cache.name());
      assertThat(config.clustering().cacheMode()).isEqualTo(CacheMode.DIST_SYNC);
      assertThat(config.memory().maxCount()).isEqualTo(200);
   }

   @Test
   void testCreateFromFile() {
      var cache = ctx.<String, String>createCacheFromFile("replicated-cache-config.xml");

      cache.on(0).put(k(), v());
      assertThat(cache.on(1).get(k())).isEqualTo(v());

      var config = ctx.manager(0).getCacheConfiguration(cache.name());
      assertThat(config.clustering().cacheMode()).isEqualTo(CacheMode.REPL_SYNC);
      assertThat(config.memory().maxCount()).isEqualTo(500);
   }

   @Test
   void testCacheNameIsTestScoped() {
      var cache1 = ctx.<String, String>createCacheFromString("""
            <local-cache name="same-name"/>
            """);
      var cache2 = ctx.<String, String>createCacheFromString("""
            <local-cache name="same-name"/>
            """);

      // Each call should produce a unique cache name
      assertThat(cache1.name()).isNotEqualTo(cache2.name());
   }
}
