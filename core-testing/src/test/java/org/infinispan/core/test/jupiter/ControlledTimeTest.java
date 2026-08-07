package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.junit.jupiter.api.Test;

/**
 * Validates ControlledTimeService integration.
 */
@InfinispanCluster(controlledTime = true)
class ControlledTimeTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   void testTimeServiceAvailable() {
      assertThat(ctx.timeService()).isNotNull();
   }

   @Test
   void testExpiration() {
      var cache = ctx.<String, String>createCache(b ->
            b.expiration().lifespan(1, TimeUnit.HOURS));

      Cache<String, String> c = cache.cache();
      c.put("key", "value");
      assertThat(c.get("key")).isEqualTo("value");

      // Advance time past the lifespan
      ctx.timeService().advance(2, TimeUnit.HOURS);

      assertThat(c.get("key")).isNull();
   }
}
