package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.getServerChannel;
import static org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCacheTestingUtil.isNearCacheHit;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.infinispan.client.hotrod.RemoteCache;
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
      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      // The server owning segment 1 (JGroups member 1) loses every primary segment — client must drop its listener
      changeOwners(ALL_ON_FIRST_OWNERS);

      eventually(() -> !primarySegments(inrc).containsKey(addr1));
      // The segment which moved is invalidated, the one which didn't move is untouched
      eventually(() -> !isNearCacheHit(clientCache, key1));
      assertThat(isNearCacheHit(clientCache, key0)).isTrue();

      // Reads now go to server 0, which is the primary owner of everything
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(inrc.getPrimaryOwner(key1)).isEqualTo(addr0);

      // Server 1 becomes a primary owner again, so the listener must be installed again on the same connection
      changeOwners(SPLIT_OWNERS);

      eventually(() -> primarySegments(inrc).containsKey(addr1));
      // Moving ownership back invalidates the segment that moved again
      eventually(() -> !isNearCacheHit(clientCache, key1));

      eventually(() -> isNearCacheHit(clientCache, key1), 15, java.util.concurrent.TimeUnit.SECONDS);
      // The new listener has to be effective: reads are cached again and server side updates invalidate them
      assertThat(clientCache.get(key1)).isEqualTo("v1");

      serverForAddress(addr1).getCacheManager().getCache().put(key1, "v1-updated");
      eventually(() -> "v1-updated".equals(clientCache.get(key1)));

      // And the other server was not affected
      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      serverForAddress(addr0).getCacheManager().getCache().put(key0, "v0-updated");
      eventually(() -> "v0-updated".equals(clientCache.get(key0)));
   }

   private int acceptedSequence(InvalidatedNearRemoteCache<?, ?> inrc) {
      OperationDispatcher dispatcher = inrc.getDispatcher();
      Object tracker = TestingUtil.extractField(dispatcher, "primaryTopologyTracker");
      synchronized (tracker) {
         java.util.Map<String, ?> topologies = TestingUtil.extractField(tracker, "primaryTopologies");
         return TestingUtil.extractField(topologies.get(inrc.getName()), "sequence");
      }
   }

   private SocketAddress[][] swappedOwners(CacheInfo info, SocketAddress addr0, SocketAddress addr1) {
      SocketAddress[][] owners = ((SegmentConsistentHash) info.getConsistentHash()).getSegmentOwners();
      SocketAddress[][] conflicting = new SocketAddress[owners.length][];
      for (int i = 0; i < owners.length; i++) {
         conflicting[i] = new SocketAddress[]{owners[i][0].equals(addr0) ? addr1 : addr0};
      }
      return conflicting;
   }

   private void push(InvalidatedNearRemoteCache<?, ?> inrc, SocketAddress source, int sequence, int baseSequence,
                     BitSet changed, int hash, SocketAddress[][] owners) {
      CacheInfo info = inrc.getDispatcher().getCacheInfo(inrc.getName());
      inrc.getDispatcher().updatePrimaryTopology(inrc.getName(), getServerChannel(inrc, source), sequence,
            baseSequence, changed, hash, info.getServers().toArray(InetSocketAddress[]::new), owners, (short) 3);
   }

   /**
    * A push with the sequence that was already accepted is ignored, whatever the owners and the hash code it carries.
    * Duplicates arrive because each primary owner pushes its own {@code CACHE_TOPOLOGY_CHANGED_EVENT}.
    */
   public void testDuplicateTopologySequenceIgnored() {
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

      CacheInfo before = inrc.getDispatcher().getCacheInfo(inrc.getName());
      int sequence = acceptedSequence(inrc);
      var statistics = clientCache.clientStatistics();
      long hits = statistics.getNearCacheHits();

      push(inrc, addr0, sequence, sequence - 1, allSegments(), before.getTopologyId() + 1, swappedOwners(before, addr0, addr1));

      assertThat(acceptedSequence(inrc)).isEqualTo(sequence);
      assertThat(inrc.getDispatcher().getCacheInfo(inrc.getName()).getTopologyId()).isEqualTo(before.getTopologyId());
      assertThat(inrc.getPrimaryOwner(key1)).isEqualTo(addr1);
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(statistics.getNearCacheHits()).as("Both entries must remain cached").isEqualTo(hits + 2);
   }

   /**
    * A push with an older sequence is ignored whatever its hash code is, as the server sequence is the only ordering.
    * Out-of-order pushes arrive when the messages of several primary owners cross on the wire.
    */
   public void testOutOfOrderTopologySequenceIgnored() {
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

      // A real topology change, so that the accepted sequence is not the first one
      changeOwners(ALL_ON_FIRST_OWNERS);
      eventually(() -> !primarySegments(inrc).containsKey(addr1));
      eventually(() -> !isNearCacheHit(clientCache, key1));

      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(isNearCacheHit(clientCache, key0)).isTrue();
      assertThat(isNearCacheHit(clientCache, key1)).isTrue();

      CacheInfo after = inrc.getDispatcher().getCacheInfo(inrc.getName());
      int sequence = acceptedSequence(inrc);
      var statistics = clientCache.clientStatistics();
      long hits = statistics.getNearCacheHits();

      // Older sequences with conflicting owners, with a hash code that is numerically larger and smaller
      push(inrc, addr0, sequence - 1, sequence - 2, allSegments(), after.getTopologyId() + 1, swappedOwners(after, addr0, addr1));
      push(inrc, addr0, sequence - 1, sequence - 2, allSegments(), after.getTopologyId() - 1, swappedOwners(after, addr0, addr1));

      assertThat(acceptedSequence(inrc)).isEqualTo(sequence);
      assertThat(inrc.getDispatcher().getCacheInfo(inrc.getName()).getTopologyId()).isEqualTo(after.getTopologyId());
      assertThat(inrc.getPrimaryOwner(key1)).isEqualTo(addr0);
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(statistics.getNearCacheHits()).as("Both entries must remain cached").isEqualTo(hits + 2);
   }

   /**
    * A newer sequence is accepted even if its hash code is numerically smaller, and fences the segments it reports
    * as changed before the topology is published.
    */
   public void testNewerSequenceWithSmallerHashIsAcceptedAndFences() {
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

      CacheInfo before = inrc.getDispatcher().getCacheInfo(inrc.getName());
      int sequence = acceptedSequence(inrc);
      var statistics = clientCache.clientStatistics();
      long hits = statistics.getNearCacheHits();
      long misses = statistics.getNearCacheMisses();
      int smaller = before.getTopologyId() - 1000;
      BitSet changed = new BitSet();
      changed.set(1);
      // The owners are the ones the client knows, as segment 1 moved and came back between the two pushes
      push(inrc, addr0, sequence + 1, sequence, changed, smaller,
            ((SegmentConsistentHash) before.getConsistentHash()).getSegmentOwners());

      assertThat(acceptedSequence(inrc)).isEqualTo(sequence + 1);
      assertThat(inrc.getDispatcher().getCacheInfo(inrc.getName()).getTopologyId()).isEqualTo(smaller);
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(statistics.getNearCacheHits()).as("Only the unchanged segment should hit the near cache").isEqualTo(hits + 1);
      assertThat(statistics.getNearCacheMisses()).as("The changed segment is fetched remotely").isEqualTo(misses + 1);
   }

   public void testUnknownTopologyHistoryInvalidatesEveryCachedSegment() {
      RemoteCacheManager clientManager = createNearClient(false);
      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;
      SocketAddress addr0 = segmentOwnerAddress(inrc, 0);
      SocketAddress addr1 = segmentOwnerAddress(inrc, 1);
      Integer key0 = findKeyForServer(inrc, addr0);
      Integer key1 = findKeyForServer(inrc, addr1);
      clientCache.put(key0, "v0");
      clientCache.put(key1, "v1");
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      var statistics = clientCache.clientStatistics();
      long initialHits = statistics.getNearCacheHits();
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(statistics.getNearCacheHits()).isEqualTo(initialHits + 2);

      CacheInfo before = inrc.getDispatcher().getCacheInfo(inrc.getName());
      int sequence = acceptedSequence(inrc);
      // The snapshot has the same owners, but its history does not reach the last accepted sequence.
      push(inrc, addr0, sequence + 2, sequence + 1, new BitSet(), before.getTopologyId(),
            ((SegmentConsistentHash) before.getConsistentHash()).getSegmentOwners());

      long misses = statistics.getNearCacheMisses();
      long hits = statistics.getNearCacheHits();
      assertThat(clientCache.get(key0)).isEqualTo("v0");
      assertThat(clientCache.get(key1)).isEqualTo("v1");
      assertThat(statistics.getNearCacheMisses()).as("Both segments must be fetched remotely").isEqualTo(misses + 2);
      assertThat(statistics.getNearCacheHits()).isEqualTo(hits);
   }

   private static BitSet allSegments() {
      BitSet all = new BitSet();
      all.set(0, SPLIT_OWNERS.length);
      return all;
   }
}
