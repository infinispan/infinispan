package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.core.test.jupiter.InfinispanExtension.k;
import static org.infinispan.core.test.jupiter.InfinispanExtension.v;

import org.infinispan.Cache;
import org.junit.jupiter.api.Test;

/**
 * Validates that a configuration file can be used as a starting point.
 */
@InfinispanCluster(config = "test-config.xml")
class ConfigFileTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   void testPredefinedCacheExists() {
      Cache<String, String> cache = ctx.manager(0).getCache("predefined");
      assertThat(cache).isNotNull();

      cache.put(k(), v());
      assertThat(cache.get(k())).isEqualTo(v());
   }

   @Test
   void testPredefinedCacheHasMaxCount() {
      var config = ctx.manager(0).getCacheConfiguration("predefined");
      assertThat(config).isNotNull();
      assertThat(config.memory().maxCount()).isEqualTo(100);
   }

   @Test
   void testCanStillCreateAdHocCaches() {
      var handle = ctx.<String, String>createCache();
      handle.cache().put(k(), v());
      assertThat(handle.cache().get(k())).isEqualTo(v());
   }
}
