package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.getServerChannel;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.isNearCacheHit;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheTopologyTest")
public class PrimaryNearCacheTopologyTest extends MultiHotRodServersTest {
   private ConfigurationBuilder configuration() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   @Override
   protected void createCacheManagers() {
      createHotRodServersWithoutClients(2, configuration());
   }

   @DataProvider
   public Object[][] bloom() {
      return new Object[][]{{false}, {true}};
   }

   private InvalidatedNearRemoteCache<Integer, String> nearCache(boolean bloom) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers) {
         builder.addServer().host(server.getHost()).port(server.getPort());
      }
      builder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      builder.remoteCache("").nearCacheMode(NearCacheMode.INVALIDATED).nearCacheMaxEntries(100)
            .nearCacheUseBloomFilter(bloom);
      RemoteCacheManager client = new RemoteCacheManager(builder.build());
      clients.add(client);
      return (InvalidatedNearRemoteCache<Integer, String>) client.<Integer, String>getCache();
   }

   @AfterMethod(alwaysRun = true)
   public void stopClients() {
      clients.forEach(RemoteCacheManager::stop);
      clients.clear();
   }

   private void advanceSequence(InvalidatedNearRemoteCache<?, ?> cache) {
      OperationDispatcher dispatcher = cache.getDispatcher();
      CacheInfo info = dispatcher.getCacheInfo("");
      dispatcher.updatePrimaryTopology("", getServerChannel(cache, info.getPrimarySegments().keySet().iterator().next()),
            1_000_000, 0, new BitSet(), info.getTopologyId(),
            info.getServers().toArray(InetSocketAddress[]::new),
            ((SegmentConsistentHash) info.getConsistentHash()).getSegmentOwners(), (short) 3);
   }

   /**
    * Traffic reconnects the transport and discovers the restarted cluster, while the lost listeners stay disabled.
    */
   private boolean allListenersDisabledWithTraffic(InvalidatedNearRemoteCache<?, ?> cache) {
      try {
         cache.get(-1);
      } catch (RuntimeException e) {
         // The servers may not be reachable yet
      }
      return !cache.hasActivePrimaryListeners();
   }

   private boolean allOwnersCacheReads(InvalidatedNearRemoteCache<Integer, String> cache) {
      Map<SocketAddress, Set<Integer>> segments = cache.getDispatcher().getPrimarySegmentsByAddress("");
      if (segments == null || segments.isEmpty()) {
         return false;
      }
      for (SocketAddress server : segments.keySet()) {
         int key = probeKey(cache, server);
         if (key < 0) {
            return false;
         }
         cache.putIfAbsent(key, "probe");
         if (!isNearCacheHit(cache, key)) {
            return false;
         }
      }
      return true;
   }

   private int probeKey(InvalidatedNearRemoteCache<Integer, String> cache, SocketAddress server) {
      for (int key = 10_000; key < 20_000; key++) {
         if (server.equals(cache.getPrimaryOwner(key))) {
            return key;
         }
      }
      return -1;
   }

   @Test(dataProvider = "bloom")
   public void testDelayedPushDoesNotChangeOwnersOrInvalidateEntries(boolean bloom) {
      InvalidatedNearRemoteCache<Integer, String> cache = nearCache(bloom);
      cache.put(1, "value");
      assertThat(cache.get(1)).isEqualTo("value");
      advanceSequence(cache);
      CacheInfo info = cache.getDispatcher().getCacheInfo("");
      SocketAddress owner = cache.getPrimaryOwner(1);
      SocketAddress other = info.getServers().stream().filter(server -> !server.equals(owner)).findFirst().orElseThrow();
      SocketAddress[][] oldOwners = new SocketAddress[info.getNumSegments()][];
      Arrays.setAll(oldOwners, segment -> new SocketAddress[]{other});
      cache.getDispatcher().updatePrimaryTopology("", getServerChannel(cache, owner), 999_999, 0, new BitSet(), info.getTopologyId() + 1,
            info.getServers().toArray(InetSocketAddress[]::new), oldOwners, (short) 3);
      assertThat(cache.getPrimaryOwner(1)).isEqualTo(owner);
      assertThat(isNearCacheHit(cache, 1)).isTrue();
      cache(0).put(1, "updated");
      eventually(() -> "updated".equals(cache.get(1)));
   }

   @Test(dataProvider = "bloom")
   public void testListenerLossStaysDisabledAcrossTopologyChanges(boolean bloom) {
      InvalidatedNearRemoteCache<Integer, String> cache = nearCache(bloom);
      SocketAddress a = InetSocketAddress.createUnresolved(server(0).getHost(), server(0).getPort());
      SocketAddress b = InetSocketAddress.createUnresolved(server(1).getHost(), server(1).getPort());
      Integer keyA = null;
      Integer keyB = null;
      for (int key = 0; key < 10000 && (keyA == null || keyB == null); key++) {
         if (a.equals(cache.getPrimaryOwner(key))) {
            keyA = key;
         }
         if (b.equals(cache.getPrimaryOwner(key))) {
            keyB = key;
         }
      }
      assertThat(keyA).isNotNull();
      assertThat(keyB).isNotNull();
      cache.put(keyA, "a");
      cache.put(keyB, "b");
      cache.get(keyA);
      cache.get(keyB);
      Channel survivingChannel = getServerChannel(cache, b);
      getServerChannel(cache, a).close().syncUninterruptibly();
      final Integer disconnectedKey = keyA;
      eventually(() -> !isNearCacheHit(cache, disconnectedKey));
      assertThat(isNearCacheHit(cache, keyB)).isTrue();
      // The segments of the lost owner are not cached, while the others still are.
      assertThat(cache.get(keyA)).isEqualTo("a");
      assertThat(isNearCacheHit(cache, keyA)).isFalse();
      assertThat(cache.get(keyB)).isEqualTo("b");
      assertThat(isNearCacheHit(cache, keyB)).isTrue();
      TestingUtil.sleepThread(1500);
      assertThat(isNearCacheHit(cache, keyA)).isFalse();

      // A real topology change adds a new owner, without retrying the disconnected existing owner.
      addHotRodServer(configuration());
      waitForClusterToForm();
      SocketAddress added = InetSocketAddress.createUnresolved(server(2).getHost(), server(2).getPort());
      eventually(() -> cache.getDispatcher().getPrimarySegmentsByAddress("").containsKey(added));
      int joinedKey = probeKey(cache, added);
      assertThat(joinedKey).isNotNegative();
      cache.put(joinedKey, "joined");
      eventually(() -> isNearCacheHit(cache, joinedKey));
      assertThat(getServerChannel(cache, b)).isSameAs(survivingChannel);
      // Select a key that still belongs to A after the rebalance.
      Integer uncachedKey = null;
      for (int key = 0; key < 10000; ++key) {
         if (a.equals(cache.getPrimaryOwner(key))) {
            uncachedKey = key;
            break;
         }
      }
      assertThat(uncachedKey).isNotNull();
      cache.put(uncachedKey, "uncached");
      assertThat(cache.get(uncachedKey)).isEqualTo("uncached");
      assertThat(isNearCacheHit(cache, uncachedKey)).isFalse();
   }

   @Test(dataProvider = "bloom")
   public void testClusterRestartBeforeShutdownTopologiesKeepsLostListenersDisabled(boolean bloom) throws Exception {
      InvalidatedNearRemoteCache<Integer, String> cache = nearCache(bloom);
      OperationDispatcher dispatcher = cache.getDispatcher();
      int initialSequence = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
      // A real join and leave advance the sequence beyond a fresh two-node cluster's sequence.
      addHotRodServer(configuration());
      waitForClusterToForm();
      killServer(2);
      waitForClusterToForm();
      int oldSequence = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
      assertThat(oldSequence).isGreaterThan(initialSequence);
      eventually(() -> primaryTopologySequence(dispatcher) == oldSequence && allOwnersCacheReads(cache));

      cache.put(1, "before-1");
      cache.put(2, "before-2");
      assertThat(cache.get(1)).isEqualTo("before-1");
      assertThat(cache.get(2)).isEqualTo("before-2");
      assertThat(isNearCacheHit(cache, 1)).isTrue();
      assertThat(isNearCacheHit(cache, 2)).isTrue();
      eventually(() -> allOwnersCacheReads(cache));

      CacheInfo info = dispatcher.getCacheInfo("");
      int oldTopologyId = info.getTopologyId();
      List<Channel> channels = info.getServers().stream().map(server -> getServerChannel(cache, server)).toList();
      List<EventLoop> eventLoops = channels.stream().map(Channel::eventLoop).distinct().toList();
      CheckPoint checkPoint = new CheckPoint();
      // Freeze the client's view before any shutdown topology or disconnect can be received.
      try {
         List<Future<Void>> blockers = eventLoops.stream().map(eventLoop -> eventLoop.<Void>submit(() -> {
            checkPoint.trigger("blocked");
            checkPoint.awaitStrict("resume", 30, java.util.concurrent.TimeUnit.SECONDS);
            return null;
         })).toList();
         checkPoint.awaitStrict("blocked", eventLoops.size(), 10, java.util.concurrent.TimeUnit.SECONDS);
         restartCluster();

         int newSequence = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
         int newTopologyId = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology()
               .getReadConsistentHash().hashCode();
         assertThat(newSequence).isLessThan(oldSequence);
         assertThat(primaryTopologySequence(dispatcher)).isEqualTo(oldSequence);
         assertThat(dispatcher.getTopologyId("")).isEqualTo(oldTopologyId);
         // Both reads must use cached values while network processing is blocked.
         assertThat(cache.getAsync(1).get(5, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("before-1");
         assertThat(cache.getAsync(2).get(5, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("before-2");
         // Only now let the client discover the lost connections. The listeners are not retried.
         checkPoint.triggerForever("resume");
         blockers.forEach(blocker -> blocker.syncUninterruptibly());
         eventually(() -> allListenersDisabledWithTraffic(cache) && dispatcher.getTopologyId("") == newTopologyId,
               30, java.util.concurrent.TimeUnit.SECONDS);
         assertThat(cache.isPrimaryMode()).isTrue();
         assertThat(cache.get(1)).isNull();
         assertThat(cache.get(2)).isNull();

         cache(0).put(1, "after");
         assertThat(cache.get(1)).isEqualTo("after");
         assertThat(isNearCacheHit(cache, 1)).isFalse();
         cache(1).put(1, "updated");
         assertThat(cache.get(1)).isEqualTo("updated");
      } finally {
         checkPoint.triggerForever("resume");
      }
   }

   private int primaryTopologySequence(OperationDispatcher dispatcher) {
      Object tracker = TestingUtil.extractField(dispatcher, "primaryTopologyTracker");
      synchronized (tracker) {
         Map<String, ?> topologies = TestingUtil.extractField(tracker, "primaryTopologies");
         return TestingUtil.extractField(topologies.get(""), "sequence");
      }
   }

   @Test(dataProvider = "bloom")
   public void testFullClusterRestartKeepsLostOwnersUncached(boolean bloom) {
      InvalidatedNearRemoteCache<Integer, String> cache = nearCache(bloom);
      cache.put(1, "before");
      assertThat(cache.get(1)).isEqualTo("before");
      advanceSequence(cache);
      restartCluster();
      eventually(() -> allListenersDisabledWithTraffic(cache), 30, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(cache.get(1)).isNull();
      cache(0).put(1, "after");
      assertThat(cache.get(1)).isEqualTo("after");
      assertThat(isNearCacheHit(cache, 1)).isFalse();
      cache(1).put(1, "updated");
      assertThat(cache.get(1)).isEqualTo("updated");
   }

   private void restartCluster() {
      int[] ports = servers.stream().mapToInt(HotRodServer::getPort).toArray();
      HotRodClientTestingUtil.killServers(servers.toArray(HotRodServer[]::new));
      servers.clear();
      TestingUtil.killCacheManagers(cacheManagers.toArray(org.infinispan.manager.EmbeddedCacheManager[]::new));
      cacheManagers.clear();
      for (int port : ports) {
         addHotRodServer(configuration(), port);
      }
      waitForClusterToForm();
   }
}
