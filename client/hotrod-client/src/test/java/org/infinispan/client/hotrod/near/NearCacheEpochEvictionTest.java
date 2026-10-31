package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

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
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests that entries stored in the near cache during primary mode are epoch-tagged, and that
 * calling {@link NearCacheService#invalidateSegment} causes the stale entry to be evicted on the
 * next {@link NearCacheService#get} call.
 */
@Test(groups = "functional", testName = "client.hotrod.near.NearCacheEpochEvictionTest")
public class NearCacheEpochEvictionTest extends MultiHotRodServersTest {

   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      managers.forEach(RemoteCacheManager::stop);
      managers.clear();
      super.destroy();
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
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

      RemoteCacheManager rcm = new RemoteCacheManager(clientBuilder.build());
      managers.add(rcm);
      return rcm;
   }

   /**
    * Verifies that after {@link NearCacheService#invalidateSegment} increments a segment's epoch,
    * a previously-cached entry for that segment is evicted on the next {@link NearCacheService#get}.
    *
    * <p>The test works by:
    * <ol>
    *   <li>Waiting for primary mode and for all primary listeners to be active.</li>
    *   <li>Confirming that NearCacheService wraps stored values with epoch metadata by checking
    *       the {@code segmentEpochs} field directly.</li>
    *   <li>Populating the near cache and confirming the entry is there.</li>
    *   <li>Calling {@code invalidateSegment} and confirming that the next {@code get} returns null.</li>
    * </ol>
    */
   public void testStaleEpochEntryIsEvictedOnGet() {
      RemoteCacheManager clientManager = createNearClient();
      RemoteCache<Integer, String> cache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) cache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      // Find a key owned by server 0
      SocketAddress addr0 = java.net.InetSocketAddress.createUnresolved(server(0).getHost(), server(0).getPort());
      Integer key = null;
      for (int i = 0; i < 10000; i++) {
         if (java.util.Objects.equals(inrc.getPrimaryOwner(i), addr0)) {
            key = i;
            break;
         }
      }
      assertThat(key).as("must find a key for server 0").isNotNull();

      NearCacheService<Integer, String> svc = inrc.getNearCacheService();

      // Confirm segmentEpochs is initialized (it is set synchronously during addNearCacheListener)
      AtomicIntegerArray segmentEpochs = TestingUtil.extractField(svc, "segmentEpochs");
      assertThat(segmentEpochs).as("segmentEpochs must be initialized in primary mode").isNotNull();

      // Populate the near cache — replace() wraps with EpochMetadataValue when segmentEpochs != null
      cache.put(key, "value");
      assertThat(cache.get(key)).isEqualTo("value");
      final Integer finalKey = key;
      eventually(() -> svc.get(finalKey) != null);
      assertThat(svc.get(key)).isNotNull();

      // Directly invalidate the segment that owns this key.
      // NearCacheService.get() unwraps EpochMetadataValue, but the raw cache entry IS an EpochMetadataValue.
      // After invalidation the epoch mismatch is detected and the entry is evicted on the next get().
      java.util.function.ToIntFunction<Object> segFn = TestingUtil.extractField(svc, "segmentFunction");
      assertThat(segFn).as("segmentFunction must be set in primary mode").isNotNull();
      int segment = segFn.applyAsInt(key);
      svc.invalidateSegment(segment);

      // After epoch mismatch, get() must evict the entry and return null
      assertThat(svc.get(key))
            .as("stale entry must be evicted after segment invalidation")
            .isNull();

      assertThat(svc.size()).isEqualTo(0);
   }

   /**
    * Verifies that after epoch invalidation, the entry is re-fetched from the server and re-cached
    * with a higher epoch counter.
    */
   public void testRepopulationAfterEpochInvalidation() {
      RemoteCacheManager clientManager = createNearClient();
      RemoteCache<Integer, String> cache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) cache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      SocketAddress addr0 = java.net.InetSocketAddress.createUnresolved(server(0).getHost(), server(0).getPort());
      Integer key = null;
      for (int i = 0; i < 10000; i++) {
         if (java.util.Objects.equals(inrc.getPrimaryOwner(i), addr0)) {
            key = i;
            break;
         }
      }
      assertThat(key).as("must find a key for server 0").isNotNull();

      NearCacheService<Integer, String> svc = inrc.getNearCacheService();
      AtomicIntegerArray segmentEpochs = TestingUtil.extractField(svc, "segmentEpochs");
      assertThat(segmentEpochs).isNotNull();

      cache.put(key, "original");
      assertThat(cache.get(key)).isEqualTo("original");
      final Integer finalKey = key;
      eventually(() -> svc.get(finalKey) != null);

      // Capture the epoch before invalidation
      java.util.function.ToIntFunction<Object> segFn = TestingUtil.extractField(svc, "segmentFunction");
      assertThat(segFn).as("segmentFunction must be set in primary mode").isNotNull();
      int segment = segFn.applyAsInt(key);
      int epochBefore = segmentEpochs.get(segment);

      // Invalidate the segment — entry becomes stale
      svc.invalidateSegment(segment);
      assertThat(svc.get(key)).as("stale entry must be evicted").isNull();
      assertThat(segmentEpochs.get(segment)).as("epoch must be incremented").isEqualTo(epochBefore + 1);

      // A cache.get() goes to server and re-populates the near cache
      assertThat(cache.get(key)).isEqualTo("original");
      eventually(() -> svc.get(finalKey) != null);

      // The new epoch in segmentEpochs is epochBefore+1; the new entry must match it
      assertThat(segmentEpochs.get(segment)).as("epoch must remain at incremented value").isEqualTo(epochBefore + 1);
   }
}
