package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.cache.CacheMode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Validates global state persistence and node restart with state recovery.
 */
@InfinispanCluster(numNodes = 2, globalState = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GlobalStateTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   @Order(1)
   void testGlobalStateEnabled() {
      var config = ctx.manager(0).getCacheManagerConfiguration();
      assertThat(config.globalState().enabled()).isTrue();
      assertThat(config.globalState().persistentLocation()).contains("node-");
   }

   @Test
   @Order(2)
   void testNodesHaveIsolatedStateDirectories() {
      var config0 = ctx.manager(0).getCacheManagerConfiguration();
      var config1 = ctx.manager(1).getCacheManagerConfiguration();
      assertThat(config0.globalState().persistentLocation())
            .isNotEqualTo(config1.globalState().persistentLocation());
   }

   @Test
   @Order(3)
   void testRestartNodeRecoversCaches() {
      // Create a replicated cache and populate it
      var cache = ctx.<String, String>createCache(b ->
            b.clustering().cacheMode(CacheMode.REPL_SYNC));

      cache.on(0).put("survive", "restart");
      cache.on(0).put("also", "survives");
      assertThat(cache.on(1).get("survive")).isEqualTo("restart");

      // Restart node 0
      ctx.restart(0);

      // Data should still be accessible through node 1
      assertThat(cache.on(1).get("survive")).isEqualTo("restart");
      assertThat(cache.on(1).get("also")).isEqualTo("survives");
   }

   @Test
   @Order(4)
   void testRestartPreservesClusterSize() {
      assertThat(ctx.numNodes()).isEqualTo(2);
      assertThat(ctx.manager(0).getMembers()).hasSize(2);
   }
}
