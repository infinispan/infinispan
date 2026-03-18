package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

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

      cache.put("key", "value");
      assertThat(cache.get("key")).isEqualTo("value");
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
      handle.cache().put("a", "b");
      assertThat(handle.cache().get("a")).isEqualTo("b");
   }
}
