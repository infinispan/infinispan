package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.core.test.jupiter.InfinispanExtension.k;
import static org.infinispan.core.test.jupiter.InfinispanExtension.v;

import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.core.test.jupiter.proto.Product;
import org.infinispan.core.test.jupiter.proto.TestProductSCI;
import org.junit.jupiter.api.Test;

/**
 * Validates cross-site replication with the test harness.
 */
@InfinispanXSite(
      value = {
            @Site(name = XSiteReplicationTest.LON, nodes = 1),
            @Site(name = XSiteReplicationTest.NYC, nodes = 1)
      },
      serializationContext = TestProductSCI.class
)
class XSiteReplicationTest {
   public static final String LON = "LON";
   public static final String NYC = "NYC";

   @InfinispanResource
   XSiteContext ctx;

   @Test
   void testSiteTopology() {
      assertThat(ctx.siteNames()).containsExactly(LON, NYC);
      assertThat(ctx.numNodes(LON)).isEqualTo(1);
      assertThat(ctx.numNodes(NYC)).isEqualTo(1);
   }

   @Test
   void testSyncBackup() {
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.SYNC));

      cache.on(LON).put(k(), v());
      assertThat(cache.on(NYC).get(k())).isEqualTo(v());
   }

   @Test
   void testDirectionalBackup() {
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backup(LON, NYC, BackupStrategy.SYNC));

      cache.on(LON).put(k(), "from-lon");
      assertThat(cache.on(NYC).get(k())).isEqualTo("from-lon");
   }

   @Test
   void testProtoEntityReplication() {
      var cache = ctx.<String, Product>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.SYNC));

      cache.on(LON).put("tablet", new Product("Tablet", 449.99));

      Product retrieved = cache.on(NYC).get("tablet");
      assertThat(retrieved).isNotNull();
      assertThat(retrieved.getName()).isEqualTo("Tablet");
      assertThat(retrieved.getPrice()).isEqualTo(449.99);
   }

   @Test
   void testTakeSiteOfflineAndOnline() {
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.SYNC));

      // Verify initial replication works
      cache.on(LON).put("before", "offline");
      assertThat(cache.on(NYC).get("before")).isEqualTo("offline");

      // Take NYC offline from LON's perspective
      ctx.sites().takeSiteOffline(cache.on(LON), NYC);
      assertThat(ctx.sites().siteStatus(cache.on(LON), NYC)).isEqualTo("offline");

      // Writes on LON no longer replicate to NYC
      cache.on(LON).put("during", "offline");
      assertThat(cache.on(NYC).get("during")).isNull();

      // Bring NYC back online
      ctx.sites().bringSiteOnline(cache.on(LON), NYC);
      assertThat(ctx.sites().siteStatus(cache.on(LON), NYC)).isEqualTo("online");

      // Replication resumes
      cache.on(LON).put("after", "online");
      assertThat(cache.on(NYC).get("after")).isEqualTo("online");
   }

   @Test
   void testAsymmetricCacheModes() {
      // LON uses DIST_SYNC, NYC uses REPL_SYNC
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.SYNC)
            .site(NYC, b -> b.clustering().cacheMode(CacheMode.REPL_SYNC)));

      // Verify each site has its own cache mode
      assertThat(cache.on(LON).getCacheConfiguration().clustering().cacheMode())
            .isEqualTo(CacheMode.DIST_SYNC);
      assertThat(cache.on(NYC).getCacheConfiguration().clustering().cacheMode())
            .isEqualTo(CacheMode.REPL_SYNC);

      // Replication still works across the asymmetric topology
      cache.on(LON).put(k(), "from-dist");
      assertThat(cache.on(NYC).get(k())).isEqualTo("from-dist");

      cache.on(NYC).put(k(2), "from-repl");
      assertThat(cache.on(LON).get(k(2))).isEqualTo("from-repl");
   }

   @Test
   void testDisconnectAndReconnectSite() {
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.SYNC));

      // Verify initial replication works
      cache.on(LON).put(k(1), v(1));
      assertThat(cache.on(NYC).get(k(1))).isEqualTo(v(1));

      // Disconnect NYC at the network level
      ctx.sites().disconnect(NYC);

      // Reconnect NYC
      ctx.sites().reconnect(NYC);

      // Replication should work again
      cache.on(LON).put(k(2), v(2));
      assertThat(cache.on(NYC).get(k(2))).isEqualTo(v(2));
   }
}
