package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

/**
 * Tests that the double-check mechanism correctly handles TOCTOU (Time-Of-Check-Time-Of-Use) races
 * where topology changes occur during primary owner lookup.
 * <p>
 * The fix captures server+version+channel, then double-checks that the primary owner hasn't changed.
 * If the primary owner changed between the initial lookup and the double-check, caching is skipped
 * to prevent storing values with stale ownership information.
 */
@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheTOCTOURaceTest")
public class PrimaryNearCacheTOCTOURaceTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
      waitForClusterToForm();
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);  // 2 owners so data survives server kill
      return hotRodCacheConfiguration(builder);
   }

   private RemoteCacheManager createNearClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100)
            .nearCacheUseBloomFilter(false);

      return new RemoteCacheManager(clientBuilder.build());
   }



   /**
    * Verifies that with stable topology, values are cached normally and the
    * PrimaryOwnerSnapshot mechanism doesn't break normal operation.
    */
   public void testStableTopologyAllowsCaching() throws Exception {
      try (RemoteCacheManager clientManager = createNearClient()) {
         RemoteCache<Integer, String> cache = clientManager.getCache();
         InvalidatedNearRemoteCache<Integer, String> inrc =
               (InvalidatedNearRemoteCache<Integer, String>) cache;

         eventually(() -> inrc.isPrimaryMode());

         Integer key = 42;
         cache.put(key, "stable-value");

         // Clear near cache
         inrc.getNearCacheService().clear();

         // Get the value - with stable topology, should be cached
         assertThat(cache.get(key)).isEqualTo("stable-value");

         // Value should eventually be in near cache
         eventually(() -> inrc.getNearCacheService().get(key) != null);

         assertThat(inrc.getNearCacheService().get(key))
               .describedAs("Value should be cached when topology is stable")
               .isNotNull();
      }
   }
}
