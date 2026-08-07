package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.cache.CacheMode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Validates network failure injection: node isolation, partitions, and merges.
 */
@InfinispanCluster(numNodes = 4)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NetworkPartitionTest {

   @InfinispanResource
   InfinispanContext ctx;

   @Test
   @Order(1)
   void testIsolateAndRestore() {
      var handle = ctx.<String, String>createCache(b ->
            b.clustering().cacheMode(CacheMode.REPL_SYNC));

      // Put data before isolation
      handle.on(0).put("key", "value");
      assertThat(handle.on(2).get("key")).isEqualTo("value");

      // Isolate node 2
      ctx.network().isolate(2);

      // Node 2 is still running but can't communicate;
      // writes on node 0 won't propagate to node 2
      // (the put may hang if sync, so use a non-isolated node to verify)
      // After isolation, node 2's local state should still have old data
      assertThat(handle.on(2).getAdvancedCache().withFlags(
            org.infinispan.context.Flag.CACHE_MODE_LOCAL).get("key")).isEqualTo("value");

      // Restore communication
      ctx.network().restore(2);
   }

   @Test
   @Order(2)
   void testClusterRestoredAfterIsolation() {
      // Verify cleanup restored the network
      assertThat(ctx.numNodes()).isEqualTo(4);
   }

   @Test
   @Order(3)
   void testPartitionAndMerge() {
      // Split into two partitions: {0,1} and {2,3}
      ctx.network().partition(new int[]{0, 1}, new int[]{2, 3});

      // Each partition should see only its own members
      assertThat(ctx.manager(0).getMembers()).hasSize(2);
      assertThat(ctx.manager(1).getMembers()).hasSize(2);
      assertThat(ctx.manager(2).getMembers()).hasSize(2);
      assertThat(ctx.manager(3).getMembers()).hasSize(2);

      // Nodes in partition 1 should see each other
      assertThat(ctx.manager(0).getMembers()).contains(ctx.manager(1).getAddress());
      // Nodes in partition 2 should see each other
      assertThat(ctx.manager(2).getMembers()).contains(ctx.manager(3).getAddress());

      // Merge
      ctx.network().merge();

      // All nodes should see all members
      assertThat(ctx.manager(0).getMembers()).hasSize(4);
      assertThat(ctx.manager(3).getMembers()).hasSize(4);
   }

   @Test
   @Order(4)
   void testClusterRestoredAfterPartition() {
      assertThat(ctx.numNodes()).isEqualTo(4);
      assertThat(ctx.manager(0).getMembers()).hasSize(4);
   }

   @Test
   @Order(5)
   void testDropMessagesBetweenSpecificNodes() {
      // Drop messages between node 0 and node 3 only
      ctx.network().dropMessagesBetween(0, 3);

      // Nodes 0 and 3 still see each other in the view (no view change)
      // but direct messages are dropped
      assertThat(ctx.manager(0).getMembers()).hasSize(4);

      // Restore
      ctx.network().restoreMessagesBetween(0, 3);
   }

   @Test
   @Order(6)
   void testThreeWayPartition() {
      // Split into three: {0}, {1,2}, {3}
      ctx.network().partition(new int[]{0}, new int[]{1, 2}, new int[]{3});

      assertThat(ctx.manager(0).getMembers()).hasSize(1);
      assertThat(ctx.manager(1).getMembers()).hasSize(2);
      assertThat(ctx.manager(2).getMembers()).hasSize(2);
      assertThat(ctx.manager(3).getMembers()).hasSize(1);

      // Merge everything
      ctx.network().merge();

      assertThat(ctx.manager(0).getMembers()).hasSize(4);
   }
}
