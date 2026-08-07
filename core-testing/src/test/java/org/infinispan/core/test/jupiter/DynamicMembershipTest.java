package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Validates dynamic cluster membership: adding and removing nodes at runtime.
 */
@InfinispanCluster(numNodes = 2)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DynamicMembershipTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   @Order(1)
   void testInitialClusterSize() {
      assertThat(ctx.numNodes()).isEqualTo(2);
   }

   @Test
   @Order(2)
   void testAddNode() {
      assertThat(ctx.numNodes()).isEqualTo(2);

      EmbeddedCacheManager newManager = ctx.addNode();
      assertThat(newManager).isNotNull();
      assertThat(ctx.numNodes()).isEqualTo(3);

      // All nodes should see 3 members
      for (int i = 0; i < 3; i++) {
         assertThat(ctx.manager(i).getMembers()).hasSize(3);
      }
   }

   @Test
   @Order(3)
   void testClusterRestoredAfterAdd() {
      // Cleanup should have restored the cluster to 2 nodes
      assertThat(ctx.numNodes()).isEqualTo(2);
   }

   @Test
   @Order(4)
   void testKillNode() {
      assertThat(ctx.numNodes()).isEqualTo(2);

      ctx.kill(1);
      assertThat(ctx.numNodes()).isEqualTo(1);
   }

   @Test
   @Order(5)
   void testClusterRestoredAfterKill() {
      // Cleanup should have restored the cluster to 2 nodes
      assertThat(ctx.numNodes()).isEqualTo(2);
      for (int i = 0; i < 2; i++) {
         assertThat(ctx.manager(i).getMembers()).hasSize(2);
      }
   }

   @Test
   @Order(6)
   void testDataSurvivesAddNode() {
      var handle = ctx.<String, String>createCache(b ->
            b.clustering().cacheMode(org.infinispan.configuration.cache.CacheMode.DIST_SYNC));

      handle.on(0).put("key1", "value1");
      assertThat(handle.on(1).get("key1")).isEqualTo("value1");

      // Add a third node and verify data is accessible
      ctx.addNode();
      assertThat(ctx.numNodes()).isEqualTo(3);

      // Define cache on new node and access data
      ctx.manager(2).defineConfiguration(handle.name(),
            new org.infinispan.configuration.cache.ConfigurationBuilder()
                  .clustering().cacheMode(org.infinispan.configuration.cache.CacheMode.DIST_SYNC).build());
      assertThat(ctx.manager(2).<String, String>getCache(handle.name()).get("key1")).isEqualTo("value1");
   }
}
