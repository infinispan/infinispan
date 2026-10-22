package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a server which stops being the primary owner of every segment, and later becomes a primary owner
 * again, gets its primary near cache listener removed and then re-installed on the same connection, and that the
 * re-installed listener properly invalidates the near cache (including when a bloom filter is used).
 */
@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheLoseAndRegainSegmentsTest")
public class PrimaryNearCacheLoseAndRegainSegmentsTest extends MultiHotRodServersTest {

   // Segment 0 is always owned by the first member, segment 1 starts on the second member
   private static final int[][] SPLIT_OWNERS = {{0}, {1}};
   // Every segment is owned by the first member, so the second member has no primary segments
   private static final int[][] ALL_ON_FIRST_OWNERS = {{0}, {0}};

   private final ControlledConsistentHashFactory.Default chFactory =
         new ControlledConsistentHashFactory.Default(SPLIT_OWNERS);
   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return ControlledConsistentHashFactory.SCI.INSTANCE;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
      waitForClusterToForm();
   }

   @AfterMethod(alwaysRun = true)
   protected void resetOwners() {
      chFactory.setOwnerIndexes(SPLIT_OWNERS);
      chFactory.triggerRebalance(cache(0));
      TestingUtil.waitForNoRebalance(caches());
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
      builder.clustering().hash().numOwners(1).numSegments(SPLIT_OWNERS.length);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(chFactory);
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

   /**
    * Returns the server address that owns segment 0 (= JGroups member 0) or segment 1 (= JGroups member 1)
    * according to the ControlledConsistentHashFactory. This is stable regardless of which test-server index
    * starts first, because the factory assigns owners by JGroups member order, not by startup order.
    */
   private SocketAddress segmentOwnerAddress(InvalidatedNearRemoteCache<?, ?> inrc, int segment) {
      SegmentConsistentHash sch = (SegmentConsistentHash) inrc.getDispatcher().getConsistentHash(inrc.getName());
      return sch.getSegmentOwners()[segment][0];
   }

   private HotRodServer serverForAddress(SocketAddress addr) {
      for (HotRodServer s : servers) {
         if (addr.equals(InetSocketAddress.createUnresolved(s.getHost(), s.getPort()))) {
            return s;
         }
      }
      throw new IllegalStateException("No server found for address " + addr);
   }

   private Integer findKeyForServer(InvalidatedNearRemoteCache<Integer, String> inrc, SocketAddress targetServer) {
      for (int i = 0; i < 10000; i++) {
         if (Objects.equals(inrc.getPrimaryOwner(i), targetServer)) {
            return i;
         }
      }
      throw new IllegalStateException("Could not find key for server " + targetServer);
   }

   private java.util.Map<SocketAddress, java.util.Set<Integer>> primarySegments(InvalidatedNearRemoteCache<?, ?> inrc) {
      return inrc.getDispatcher().getPrimarySegmentsByAddress(inrc.getName());
   }

   private void changeOwners(int[][] owners) {
      chFactory.setOwnerIndexes(owners);
      chFactory.triggerRebalance(cache(0));
      TestingUtil.waitForNoRebalance(caches());
   }

   public void testServerLosesAndRegainsAllPrimarySegments() {
      doTestLoseAndRegain(false);
   }

   public void testServerLosesAndRegainsAllPrimarySegmentsWithBloomFilter() {
      doTestLoseAndRegain(true);
   }

   private void doTestLoseAndRegain(boolean bloom) {
      RemoteCacheManager clientManager = createNearClient(bloom);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      // addr0 = JGroups member 0's Hot Rod address (owns segment 0 in SPLIT_OWNERS)
      // addr1 = JGroups member 1's Hot Rod address (owns segment 1 in SPLIT_OWNERS)
      // This derives the addresses from the actual consistent hash rather than from the test-server
      // startup order, which is independent of JGroups member order.
      SocketAddress addr0 = segmentOwnerAddress(inrc, 0);
      SocketAddress addr1 = segmentOwnerAddress(inrc, 1);

      Integer key0 = findKeyForServer(inrc, addr0);
      Integer key1 = findKeyForServer(inrc, addr1);

      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();
      eventually(() -> inrc.getServerListenerState(addr1) != null && inrc.getServerListenerState(addr1).isActive());

      // The server owning segment 1 (JGroups member 1) loses every primary segment — client must drop its listener
      changeOwners(ALL_ON_FIRST_OWNERS);

      eventually(() -> !primarySegments(inrc).containsKey(addr1));
      eventually(() -> inrc.getServerListenerState(addr1) == null);
      // The segment which moved is invalidated, the one which didn't move is untouched
      eventually(() -> inrc.getNearCacheService().get(key1) == null);
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();

      // Reads now go to server 0, which is the primary owner of everything
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(inrc.getPrimaryOwner(key1)).isEqualTo(addr0);

      // Server 1 becomes a primary owner again, so the listener must be installed again on the same connection
      changeOwners(SPLIT_OWNERS);

      eventually(() -> primarySegments(inrc).containsKey(addr1));
      eventually(() -> inrc.getServerListenerState(addr1) != null && inrc.getServerListenerState(addr1).isActive(),
            15, java.util.concurrent.TimeUnit.SECONDS);
      // Moving ownership back invalidates the segment that moved again
      eventually(() -> inrc.getNearCacheService().get(key1) == null);

      // The new listener has to be effective: reads are cached again and server side updates invalidate them
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      serverForAddress(addr1).getCacheManager().getCache().put(key1, "v1-updated");
      eventually(() -> inrc.getNearCacheService().get(key1) == null);
      assertThat(clientCache.get(key1)).isEqualTo("v1-updated");

      // And the other server was not affected
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      serverForAddress(addr0).getCacheManager().getCache().put(key0, "v0-updated");
      eventually(() -> inrc.getNearCacheService().get(key0) == null);
      assertThat(clientCache.get(key0)).isEqualTo("v0-updated");
   }

   /**
    * A topology event with the same ID as the last-seen topology must be silently ignored.
    * Duplicate events can arrive because each primary-owner node pushes its own
    * {@code CACHE_TOPOLOGY_CHANGED_EVENT}, so the client may receive the same topology ID
    * from several channels simultaneously.
    */
   public void testDuplicateTopologyIdIgnored() {
      RemoteCacheManager clientManager = createNearClient(false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      SocketAddress addr0 = segmentOwnerAddress(inrc, 0);
      SocketAddress addr1 = segmentOwnerAddress(inrc, 1);

      Integer key0 = findKeyForServer(inrc, addr0);
      Integer key1 = findKeyForServer(inrc, addr1);

      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Capture the current CacheInfo (topology ID = N) so we can replay it
      CacheInfo currentCacheInfo = inrc.getDispatcher().getCacheInfo(inrc.getName());
      int currentTopologyId = currentCacheInfo.getTopologyId();
      assertThat(currentTopologyId).isGreaterThan(0);

      // Replay the same topology event a second time with the same ID.
      // onTopologyUpdate must treat it as stale and not invalidate any segments.
      @SuppressWarnings("unchecked")
      BiConsumer<CacheInfo, CacheInfo> topologyListener =
            TestingUtil.extractField(inrc, "topologyListener");
      topologyListener.accept(currentCacheInfo, currentCacheInfo);

      // Both entries must still be in the near cache — the duplicate event must not have invalidated them
      assertThat(inrc.getNearCacheService().get(key0))
            .as("key0 must remain cached after a duplicate topology event")
            .isNotNull();
      assertThat(inrc.getNearCacheService().get(key1))
            .as("key1 must remain cached after a duplicate topology event")
            .isNotNull();
   }

   /**
    * A topology event with an older ID than the last-seen one must be silently ignored.
    * Out-of-order events can arrive when several primary-owner nodes push topology changes
    * asynchronously and their messages cross on the wire.
    */
   public void testOutOfOrderTopologyIdIgnored() {
      RemoteCacheManager clientManager = createNearClient(false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      SocketAddress addr0 = segmentOwnerAddress(inrc, 0);
      SocketAddress addr1 = segmentOwnerAddress(inrc, 1);

      Integer key0 = findKeyForServer(inrc, addr0);
      Integer key1 = findKeyForServer(inrc, addr1);

      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Trigger a real topology change so that currentTopologyId > 1
      changeOwners(ALL_ON_FIRST_OWNERS);
      eventually(() -> !primarySegments(inrc).containsKey(addr1));
      // Wait for the near-cache topology listener to process the update: once key1 is evicted,
      // onTopologyUpdate has run and lastSeenTopologyId is already set to the new topology ID.
      eventually(() -> inrc.getNearCacheService().get(key1) == null);

      CacheInfo afterRebalance = inrc.getDispatcher().getCacheInfo(inrc.getName());
      int currentTopologyId = afterRebalance.getTopologyId();
      assertThat(currentTopologyId).isGreaterThan(1);

      // Re-populate near cache (key1 moved, read it again from server 0)
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      clientCache.get(key1); // repopulate
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();
      assertThat(inrc.getNearCacheService().get(key1)).isNotNull();

      // Now inject an event with an older topology ID — owners are the same as after rebalance,
      // but the ID is older so the event must be silently dropped.
      CacheInfo olderCacheInfo = afterRebalance.withNewServers(currentTopologyId - 1, afterRebalance.getServers());

      @SuppressWarnings("unchecked")
      BiConsumer<CacheInfo, CacheInfo> topologyListener =
            TestingUtil.extractField(inrc, "topologyListener");
      topologyListener.accept(afterRebalance, olderCacheInfo);

      // Both entries must still be in the near cache — the out-of-order event must not have invalidated them
      assertThat(inrc.getNearCacheService().get(key0))
            .as("key0 must remain cached after an out-of-order topology event")
            .isNotNull();
      assertThat(inrc.getNearCacheService().get(key1))
            .as("key1 must remain cached after an out-of-order topology event")
            .isNotNull();
   }
}
