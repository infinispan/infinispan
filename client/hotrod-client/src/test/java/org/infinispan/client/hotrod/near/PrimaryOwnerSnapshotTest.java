package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

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
 * Verifies that PrimaryOwnerSnapshot prevents TOCTOU races where topology changes
 * during primary owner lookup could result in caching values without active listeners.
 */
@Test(groups = "functional", testName = "client.hotrod.near.PrimaryOwnerSnapshotTest")
public class PrimaryOwnerSnapshotTest extends MultiHotRodServersTest {

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
            .nearCacheMaxEntries(100);

      return new RemoteCacheManager(clientBuilder.build());
   }

   /**
    * Tests that values cached in primary mode have active listeners on the current primary owner.
    */
   public void testCachedValuesHaveActiveListeners() throws Exception {
      try (RemoteCacheManager rcm = createNearClient()) {
         RemoteCache<Integer, String> cache = rcm.getCache();
         InvalidatedNearRemoteCache<Integer, String> inrc =
               (InvalidatedNearRemoteCache<Integer, String>) cache;

         // Wait for primary mode to activate
         eventually(() -> inrc.isPrimaryMode(), 10, TimeUnit.SECONDS);
         assertThat(inrc.isPrimaryMode()).isTrue();

         // Put and get a value
         cache.put(1, "value1");
         assertThat(cache.get(1)).isEqualTo("value1");

         // Wait for near cache population
         eventually(() -> inrc.getNearCacheService().get(1) != null, 10, TimeUnit.SECONDS);

         // Verify that if a value is cached, it has an active listener on the primary owner
         Object cached = inrc.getNearCacheService().get(1);
         if (cached != null) {
            SocketAddress owner = inrc.getPrimaryOwner(1);
            assertThat(owner).as("Primary owner must not be null for cached value").isNotNull();

            InvalidatedNearRemoteCache.ServerListenerState state = inrc.getServerListenerState(owner);
            assertThat(state).as("Must have listener state for primary owner").isNotNull();
            assertThat(state.isActive()).as("Listener must be active on primary owner").isTrue();
         }
      }
   }

   /**
    * Tests that after topology change, cached values maintain listener consistency.
    */
   public void testSnapshotConsistencyAfterTopologyChange() throws Exception {
      try (RemoteCacheManager rcm = createNearClient()) {
         RemoteCache<Integer, String> cache = rcm.getCache();
         InvalidatedNearRemoteCache<Integer, String> inrc =
               (InvalidatedNearRemoteCache<Integer, String>) cache;

         // Wait for primary mode
         eventually(() -> inrc.isPrimaryMode(), 10, TimeUnit.SECONDS);

         // Put and cache a value
         cache.put(1, "value1");
         assertThat(cache.get(1)).isEqualTo("value1");

         // Clear near cache and kill a server to trigger topology change
         inrc.getNearCacheService().clear();
         killServer(1);

         // Wait for listener to be established on new primary
         eventually(() -> {
            SocketAddress owner = inrc.getPrimaryOwner(1);
            if (owner == null) return false;
            InvalidatedNearRemoteCache.ServerListenerState state = inrc.getServerListenerState(owner);
            return state != null && state.isActive();
         }, 10, TimeUnit.SECONDS);

         // Get the value again - should succeed since we have 2 owners
         String value = cache.get(1);
         assertThat(value).as("Value should survive server kill with numOwners=2").isEqualTo("value1");

         // If value is now cached, verify listener consistency
         Object cached = inrc.getNearCacheService().get(1);
         if (cached != null) {
            SocketAddress currentOwner = inrc.getPrimaryOwner(1);
            assertThat(currentOwner).as("Current primary owner must not be null").isNotNull();

            InvalidatedNearRemoteCache.ServerListenerState state = inrc.getServerListenerState(currentOwner);
            assertThat(state).as("Must have listener on current primary owner").isNotNull();
            assertThat(state.isActive()).as("Listener on current primary must be active").isTrue();
         }
      }
   }
}
