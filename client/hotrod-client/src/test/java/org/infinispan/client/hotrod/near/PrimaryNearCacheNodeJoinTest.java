package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.isNearCacheHit;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

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

@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheNodeJoinTest")
public class PrimaryNearCacheNodeJoinTest extends MultiHotRodServersTest {

   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
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

   private RemoteCacheManager createNearClient(boolean bloom) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100)
            .nearCacheUseBloomFilter(bloom);

      RemoteCacheManager rcm = new RemoteCacheManager(clientBuilder.build());
      managers.add(rcm);
      return rcm;
   }

   private SocketAddress serverSocketAddress(int serverIndex) {
      return java.net.InetSocketAddress.createUnresolved(server(serverIndex).getHost(), server(serverIndex).getPort());
   }

   private Integer findKeyForServer(InvalidatedNearRemoteCache<Integer, String> inrc, SocketAddress targetServer) {
      for (int i = 10_000; i < 20_000; i++) {
         if (java.util.Objects.equals(inrc.getPrimaryOwner(i), targetServer)) {
            return i;
         }
      }
      throw new IllegalStateException("Could not find key for server " + targetServer);
   }

   public void testNodeJoinWithNearCache() {
      doTestNodeJoin(false);
   }

   public void testNodeJoinWithBloomFilterNearCache() {
      doTestNodeJoin(true);
   }

   public void testMovedSegmentsInvalidatedOnNodeJoin() {
      RemoteCacheManager clientManager = createNearClient(true);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      java.util.Map<Integer, SocketAddress> initialOwners = new java.util.HashMap<>();
      for (int i = 0; i < 50; i++) {
         clientCache.put(i, "val" + i);
      }
      for (int i = 0; i < 50; i++) {
         final int key = i;
         eventually(() -> {
            assertThat(clientCache.get(key)).isEqualTo("val" + key);
            return isNearCacheHit(clientCache, key);
         });
         initialOwners.put(i, inrc.getPrimaryOwner(i));
      }

      // Add a 3rd server to the cluster
      HotRodServer server2 = addHotRodServer(getCacheConfiguration());
      server2.getCacheManager().getCache();
      TestingUtil.waitForNoRebalance(caches());

      try {
         SocketAddress addr2 = java.net.InetSocketAddress.createUnresolved(server2.getHost(), server2.getPort());
         eventually(() -> inrc.getDispatcher().getPrimarySegmentsByAddress(inrc.getName()).containsKey(addr2));
         // Probe the new owner outside the keys whose invalidation is checked below.
         Integer probeKey = findKeyForServer(inrc, addr2);
         clientCache.put(probeKey, "joined");
         eventually(() -> isNearCacheHit(clientCache, probeKey));

         int movedCount = 0;
         int unmovedCount = 0;
         for (int i = 0; i < 50; i++) {
            SocketAddress newOwner = inrc.getPrimaryOwner(i);
            if (!java.util.Objects.equals(initialOwners.get(i), newOwner)) {
               movedCount++;
               final int key = i;
               eventually(() -> !isNearCacheHit(clientCache, key));
            } else {
               unmovedCount++;
               assertThat(isNearCacheHit(clientCache, i))
                     .as("Key %d with unchanged primary owner should remain cached", i)
                     .isTrue();
            }
         }
         assertThat(movedCount).as("Some segments should have moved to joined node").isGreaterThan(0);
         assertThat(unmovedCount).as("Some segments should have stayed on original nodes").isGreaterThan(0);
      } finally {
         HotRodClientTestingUtil.killServers(server2);
         servers.remove(server2);
         TestingUtil.killCacheManagers(server2.getCacheManager());
         cacheManagers.remove(server2.getCacheManager());
         TestingUtil.waitForNoRebalance(caches());
      }
   }

   private void doTestNodeJoin(boolean bloom) {
      RemoteCacheManager clientManager = createNearClient(bloom);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "val0");
      clientCache.put(key1, "val1");

      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(clientCache.get(key1)).isEqualTo("val1");

      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      // Add a 3rd server to the cluster
      HotRodServer server2 = addHotRodServer(getCacheConfiguration());
      server2.getCacheManager().getCache();
      TestingUtil.waitForNoRebalance(caches());

      try {
         SocketAddress addr2 = java.net.InetSocketAddress.createUnresolved(server2.getHost(), server2.getPort());
         eventually(() -> inrc.getDispatcher().getPrimarySegmentsByAddress(inrc.getName()).containsKey(addr2));

         Integer key2 = findKeyForServer(inrc, addr2);
         clientCache.put(key2, "val2");
         eventually(() -> isNearCacheHit(clientCache, key2));
         assertThat(clientCache.get(key2)).isEqualTo("val2");

         // Direct update on server 2 sends invalidation to near cache
         server2.getCacheManager().getCache().put(key2, "val2-updated");
         eventually(() -> "val2-updated".equals(clientCache.get(key2)));
      } finally {
         HotRodClientTestingUtil.killServers(server2);
         servers.remove(server2);
         TestingUtil.killCacheManagers(server2.getCacheManager());
         cacheManagers.remove(server2.getCacheManager());
         TestingUtil.waitForNoRebalance(caches());
      }
   }
}
