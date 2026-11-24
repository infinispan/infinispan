package org.infinispan.core.test.jupiter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.CacheMode;
import org.junit.jupiter.api.Test;

/**
 * Validates that disconnecting a single site does not affect communication
 * between the remaining sites.
 */
@InfinispanXSite(value = {
      @Site(name = ThreeSiteDisconnectTest.LON, nodes = 1),
      @Site(name = ThreeSiteDisconnectTest.NYC, nodes = 1),
      @Site(name = ThreeSiteDisconnectTest.SFO, nodes = 1)
})
class ThreeSiteDisconnectTest {

   public static final String LON = "LON";
   public static final String NYC = "NYC";
   public static final String SFO = "SFO";

   @InfinispanResource
   XSiteContext ctx;

   @Test
   void testDisconnectOnlyAffectsTargetSite() {
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.ASYNC));

      // All sites can communicate initially
      cache.on(LON).put("initial", "value");
      Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
         assertThat(cache.on(NYC).get("initial")).isEqualTo("value");
         assertThat(cache.on(SFO).get("initial")).isEqualTo("value");
      });

      // Disconnect NYC: LON and SFO must still be able to communicate
      ctx.sites().disconnect(NYC);

      cache.on(LON).put("from-lon", "value");
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(cache.on(SFO).get("from-lon")).isEqualTo("value"));

      cache.on(SFO).put("from-sfo", "value");
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(cache.on(LON).get("from-sfo")).isEqualTo("value"));

      // NYC did not receive anything while disconnected
      Awaitility.await().during(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(cache.on(NYC).get("from-lon")).isNull());

      // After reconnecting NYC, replication to it resumes
      ctx.sites().reconnect(NYC);
      cache.on(LON).put("after-reconnect", "value");
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(cache.on(NYC).get("after-reconnect")).isEqualTo("value"));
   }

   @Test
   void testReconnectPreservesOtherDisconnections() {
      var cache = ctx.<String, String>createCache(c -> c
            .cacheMode(CacheMode.DIST_SYNC)
            .backups(BackupStrategy.ASYNC));

      cache.on(LON).put("initial", "value");
      Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
         assertThat(cache.on(NYC).get("initial")).isEqualTo("value");
         assertThat(cache.on(SFO).get("initial")).isEqualTo("value");
      });

      // Disconnect both NYC and SFO
      ctx.sites().disconnect(NYC);
      ctx.sites().disconnect(SFO);

      cache.on(LON).put("both-offline", "value");
      Awaitility.await().during(1, TimeUnit.SECONDS).untilAsserted(() -> {
         assertThat(cache.on(NYC).get("both-offline")).isNull();
         assertThat(cache.on(SFO).get("both-offline")).isNull();
      });

      // Reconnecting NYC must not reconnect SFO as well
      ctx.sites().reconnect(NYC);
      cache.on(LON).put("nyc-online", "value");
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(cache.on(NYC).get("nyc-online")).isEqualTo("value"));
      Awaitility.await().during(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(cache.on(SFO).get("nyc-online")).isNull());
   }
}
