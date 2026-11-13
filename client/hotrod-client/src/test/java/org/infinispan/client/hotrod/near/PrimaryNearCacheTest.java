package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.getServerChannel;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.isNearCacheHit;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.reflect.Proxy;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheTest")
public class PrimaryNearCacheTest extends MultiHotRodServersTest {

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

   private RemoteCacheManager createNearClient(ClientIntelligence intelligence, boolean bloom) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(intelligence);
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
      for (int i = 0; i < 10000; i++) {
         if (java.util.Objects.equals(inrc.getPrimaryOwner(i), targetServer)) {
            return i;
         }
      }
      throw new IllegalStateException("Could not find key for server " + targetServer);
   }

   public void testPrimaryNearCacheOperations() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      assertThat(clientCache).isInstanceOf(InvalidatedNearRemoteCache.class);
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "val0");
      clientCache.put(key1, "val1");

      // Reads populate near cache
      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(clientCache.get(key1)).isEqualTo("val1");

      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      // Update from server directly
      server(0).getCacheManager().getCache().put(key0, "val0-updated");
      eventually(() -> "val0-updated".equals(clientCache.get(key0)));

      // key1 on server 1 remains cached
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();
   }

   public void testPrimaryNearCacheOperationsWithBloomFilter() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, true);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      assertThat(clientCache).isInstanceOf(InvalidatedNearRemoteCache.class);
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "val0");
      clientCache.put(key1, "val1");

      // Reads populate near cache
      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(clientCache.get(key1)).isEqualTo("val1");

      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      // Update from server directly
      server(0).getCacheManager().getCache().put(key0, "val0-updated");
      eventually(() -> "val0-updated".equals(clientCache.get(key0)));

      // key1 on server 1 remains cached
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();
   }

   public void testSegmentInvalidationOnServerStop() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");

      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");

      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      int port1 = server(1).getPort();
      try {
         // Stop server 1 - should only invalidate segments for server 1, not server 0!
         HotRodClientTestingUtil.killServers(server(1));

         eventually(() -> !isNearCacheHit(clientCache, key1));

         // Key 0 on surviving server 0 MUST STILL BE CACHED!
         assertThat(isNearCacheHit(clientCache, key0))
               .as("Key on surviving server should remain cached in near cache")
               .isTrue();
         assertThat(clientCache.get(key0)).isEqualTo("v0");
      } finally {
         HotRodServer restartedServer1 = HotRodClientTestingUtil.startHotRodServer(manager(1), port1, new HotRodServerConfigurationBuilder());
         servers.set(1, restartedServer1);
      }
   }

   public void testServerRestartDoesNotReconnectPrimaryListenerWithoutTopology() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      Integer key1 = findKeyForServer(inrc, serverSocketAddress(1));

      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");

      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");

      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      int port1 = server(1).getPort();

      // Kill server 1
      HotRodClientTestingUtil.killServers(server(1));

      // Key 1 is invalidated in near cache, key 0 remains cached
      eventually(() -> !isNearCacheHit(clientCache, key1));
      assertThat(isNearCacheHit(clientCache, key0)).isTrue();

      // Restart server 1 on the same port
      HotRodServer restartedServer1 = HotRodClientTestingUtil.startHotRodServer(manager(1), port1, new HotRodServerConfigurationBuilder());
      servers.set(1, restartedServer1);

      // Reopening the transport does not make this owner's reads cacheable while it remains a primary owner.
      TestingUtil.sleepThread(1500);
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(isNearCacheHit(clientCache, key1)).isFalse();
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
   }

   public void testFallbackWhenNotHashAware() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.TOPOLOGY_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isFalse();
   }

   public void testRetriedReadNotCachedWithBloomFilter() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, true);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      clientCache.put(key0, "val0");

      RemoteCacheImpl<Integer, String> rci = TestingUtil.extractField(inrc, "delegate");
      CacheOperationsFactory originalCof = TestingUtil.extractField(rci, "operationsFactory");
      Channel server1Channel = getServerChannel(inrc, serverSocketAddress(1));

      CacheOperationsFactory wrappedCof = (CacheOperationsFactory) Proxy.newProxyInstance(
            CacheOperationsFactory.class.getClassLoader(),
            new Class<?>[]{CacheOperationsFactory.class},
            (proxy, method, args) -> {
               if ("newGetWithMetadataOperation".equals(method.getName()) && args != null && args.length == 2) {
                  return originalCof.newGetWithMetadataOperation(args[0], server1Channel);
               }
               return method.invoke(originalCof, args);
            }
      );
      TestingUtil.replaceField(wrappedCof, "operationsFactory", rci, RemoteCacheImpl.class);
      try {
         assertThat(clientCache.get(key0)).isEqualTo("val0");
         assertThat(isNearCacheHit(clientCache, key0)).isFalse();
      } finally {
         TestingUtil.replaceField(originalCof, "operationsFactory", rci, RemoteCacheImpl.class);
      }
   }

   public void testRetriedReadCachedWhenBloomFilterDisabled() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      Integer key0 = findKeyForServer(inrc, serverSocketAddress(0));
      clientCache.put(key0, "val0");

      RemoteCacheImpl<Integer, String> rci = TestingUtil.extractField(inrc, "delegate");
      CacheOperationsFactory originalCof = TestingUtil.extractField(rci, "operationsFactory");
      Channel server1Channel = getServerChannel(inrc, serverSocketAddress(1));

      CacheOperationsFactory wrappedCof = (CacheOperationsFactory) Proxy.newProxyInstance(
            CacheOperationsFactory.class.getClassLoader(),
            new Class<?>[]{CacheOperationsFactory.class},
            (proxy, method, args) -> {
               if ("newGetWithMetadataOperation".equals(method.getName()) && args != null && args.length == 2) {
                  return originalCof.newGetWithMetadataOperation(args[0], server1Channel);
               }
               return method.invoke(originalCof, args);
            }
      );
      TestingUtil.replaceField(wrappedCof, "operationsFactory", rci, RemoteCacheImpl.class);
      try {
         assertThat(clientCache.get(key0)).isEqualTo("val0");
         assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      } finally {
         TestingUtil.replaceField(originalCof, "operationsFactory", rci, RemoteCacheImpl.class);
      }
   }

   public void testConcurrentBloomFilterUpdatesInPrimaryMode() {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, true);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      // Trigger multiple concurrent bloom filter updates
      CompletableFuture<?>[] futures = new CompletableFuture<?>[10];
      for (int i = 0; i < futures.length; i++) {
         futures[i] = inrc.updateBloomFilter().toCompletableFuture();
      }
      CompletableFuture.allOf(futures).join();

      // Every owner must still allow reads to be cached after the concurrent updates finish.
      for (int i = 0; i < servers.size(); i++) {
         Integer key = findKeyForServer(inrc, serverSocketAddress(i));
         clientCache.put(key, "value");
         assertThat(clientCache.get(key)).isEqualTo("value");
         var statistics = clientCache.clientStatistics();
         long hits = statistics.getNearCacheHits();
         assertThat(clientCache.get(key)).isEqualTo("value");
         assertThat(statistics.getNearCacheHits()).isEqualTo(hits + 1);

         // Advertise the cached key and verify that its owner's listener still invalidates it.
         inrc.updateBloomFilter().toCompletableFuture().join();
         server(i).getCacheManager().getCache().put(key, "updated");
         eventually(() -> "updated".equals(clientCache.get(key)));
      }
   }
}
