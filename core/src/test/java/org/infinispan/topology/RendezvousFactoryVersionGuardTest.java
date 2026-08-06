package org.infinispan.topology;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.impl.HistoryHintedRendezvousConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.statetransfer.StateTransferTracker;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.TestingEventLogManager;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests that {@link ClusterCacheStatus#isUsingFallbackFactory()} correctly tracks the
 * coordinator-side version guard in {@code effectiveConsistentHashFactory()}.
 *
 * <p>Scenario: a cache is configured with {@link HistoryHintedRendezvousConsistentHashFactory}.
 * While the cluster contains at least one node older than 16.3 the coordinator must activate
 * the {@link org.infinispan.distribution.ch.impl.SyncConsistentHashFactory} fallback; once all
 * nodes reach 16.3 or above it must switch back to the configured factory.</p>
 */
@Test(groups = "unit", testName = "topology.RendezvousFactoryVersionGuardTest")
public class RendezvousFactoryVersionGuardTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "testCache";

   /**
    * CacheJoinInfo that carries HistoryHintedRendezvousConsistentHashFactory — the factory a
    * 16.3 node embeds in its join request for a distributed cache.
    */
   private static final CacheJoinInfo HHRCHF_JOIN_INFO =
         new CacheJoinInfo(HistoryHintedRendezvousConsistentHashFactory.getInstance(),
               256, 2, 10_000, CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());

   private static final Address A = Address.random("A");
   private static final Address B = Address.random("B");

   private ClusterCacheStatus status;
   private ClusterTopologyManagerImpl topologyManager;
   private Transport transport;
   private MockitoSession mockitoSession;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession().strictness(Strictness.LENIENT).startMocking();

      EventLogManager eventLogManager = new TestingEventLogManager();
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      topologyManager = mock(ClusterTopologyManagerImpl.class);
      transport = mock(Transport.class);
      StateTransferTracker stateTransferTracker = mock(StateTransferTracker.class);
      when(stateTransferTracker.forCache(any()))
            .thenReturn(mock(StateTransferTracker.CacheStateTransferTracker.class));

      PreferAvailabilityStrategy availabilityStrategy =
            new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager);

      status = new ClusterCacheStatus(cacheManager, null, CACHE_NAME, availabilityStrategy,
            RebalanceType.FOUR_PHASE, topologyManager, transport, stateTransferTracker,
            persistentUUIDManager, eventLogManager, Optional.empty(), false);

      // By default rebalancing is enabled so that the topology operations complete normally
      when(topologyManager.isRebalancingEnabled()).thenReturn(true);
   }

   @AfterMethod(alwaysRun = true)
   public void teardown() {
      mockitoSession.finishMocking();
   }

   /**
    * While the oldest cluster member is below 16.3 the coordinator must switch to the Sync
    * fallback — {@code isUsingFallbackFactory()} must return {@code true}.
    */
   public void testFallbackActiveWhenOldNodePresent() {
      // Oldest member is 16.2 — below the 16.3 threshold
      NodeVersion oldVersion = NodeVersion.from((byte) 16, (byte) 2, (byte) 0);
      when(transport.getOldestMember()).thenReturn(oldVersion);

      // Trigger effectiveConsistentHashFactory() via the first join, which calls createInitialCacheTopology()
      status.doJoin(A, joinInfoFor(A));

      assertTrue(status.isUsingFallbackFactory(),
            "Fallback must be active while oldest member (" + oldVersion + ") is below 16.3");
   }

   /**
    * When the oldest member is exactly 16.3 the threshold is met and the configured
    * HistoryHintedRendezvousConsistentHashFactory must be used — no fallback.
    */
   public void testNoFallbackWhenAllNodesAtMinVersion() {
      when(transport.getOldestMember()).thenReturn(NodeVersion.SIXTEEN_THREE);

      status.doJoin(A, joinInfoFor(A));

      assertFalse(status.isUsingFallbackFactory(),
            "Fallback must NOT be active when oldest member is exactly 16.3");
   }

   /**
    * Simulates the full rolling-upgrade lifecycle on the coordinator:
    * <ol>
    *   <li>Cluster starts mixed (old node present) → fallback active.</li>
    *   <li>Old node leaves, cluster becomes all-16.3+ → fallback deactivated.</li>
    * </ol>
    *
    * <p>This is the critical path for the rolling-upgrade fix: once the last old node leaves
    * the coordinator must switch back to {@link HistoryHintedRendezvousConsistentHashFactory}
    * and trigger a rebalance that redistributes ownership using the new strategy.</p>
    */
   public void testFallbackDeactivatesAfterUpgradeCompletes() {
      // --- Phase 1: mixed cluster, old node is oldest ---
      NodeVersion oldVersion = NodeVersion.from((byte) 16, (byte) 2, (byte) 0);
      when(transport.getOldestMember()).thenReturn(oldVersion);

      status.doJoin(A, joinInfoFor(A));

      assertTrue(status.isUsingFallbackFactory(),
            "Phase 1: fallback must be active in mixed-version cluster (oldest=" + oldVersion + ")");

      // --- Phase 2: all nodes upgraded, oldest is now 16.3 ---
      when(transport.getOldestMember()).thenReturn(NodeVersion.SIXTEEN_THREE);

      // A topology change (second node joining) exercises effectiveConsistentHashFactory() again
      status.doJoin(B, joinInfoFor(B));

      assertFalse(status.isUsingFallbackFactory(),
            "Phase 2: fallback must be deactivated once all nodes are >= 16.3");
   }

   /**
    * A cache configured with a non-Rendezvous factory must never activate the fallback,
    * regardless of the cluster version.
    */
   public void testNonRendezvousFactoryNeverUsesFallback() {
      // Use DefaultConsistentHashFactory — not a PureRendezvousConsistentHashFactory
      CacheJoinInfo defaultInfo = new CacheJoinInfo(
            org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory.getInstance(),
            256, 2, 10_000, CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());

      // Even with an old-version oldest member ...
      NodeVersion oldVersion = NodeVersion.from((byte) 16, (byte) 2, (byte) 0);
      when(transport.getOldestMember()).thenReturn(oldVersion);

      status.doJoin(A, new CacheJoinInfo(defaultInfo.getConsistentHashFactory(),
            defaultInfo.getNumSegments(), defaultInfo.getNumOwners(),
            defaultInfo.getTimeout(), defaultInfo.getCacheMode(), defaultInfo.getCapacityFactor(),
            new UUID(A.hashCode(), A.hashCode()), Optional.empty()));

      assertFalse(status.isUsingFallbackFactory(),
            "Non-Rendezvous factory must never activate the fallback");
   }

   /**
    * Simulates coordinator election where the first recovered node had a downgraded
    * {@link SyncConsistentHashFactory} (sent during a mixed-version upgrade) but a later
    * recovered node carries {@link HistoryHintedRendezvousConsistentHashFactory}.
    *
    * <p>The fix in {@code addMember()} must promote {@code this.joinInfo} to the HHRCHF-bearing
    * one so that {@code effectiveConsistentHashFactory()} can activate HHRCHF once all nodes
    * are >= 16.3.</p>
    */
   public void testJoinInfoUpgradedWhenHHRCHFArrivesAfterSyncDuringRecovery() {
      // Both nodes are all-16.3 after the upgrade
      when(transport.getOldestMember()).thenReturn(NodeVersion.SIXTEEN_THREE);

      // First member reports SyncConsistentHashFactory (the downgraded factory it stored at join time)
      CacheJoinInfo syncInfo = new CacheJoinInfo(
            SyncConsistentHashFactory.getInstance(),
            256, 2, 10_000, CacheMode.DIST_SYNC, 1.0f,
            new UUID(A.hashCode(), A.hashCode()), Optional.empty());
      status.doJoin(A, syncInfo);

      // Confirm the stored joinInfo is currently Sync (i.e. first-member wins by default)
      assertInstanceOf(SyncConsistentHashFactory.class,
            status.getJoinInfo().getConsistentHashFactory(),
            "After first join with Sync, stored factory should be SyncConsistentHashFactory");

      // Second member joins with HHRCHF — addMember() must upgrade this.joinInfo
      status.doJoin(B, joinInfoFor(B));

      assertInstanceOf(HistoryHintedRendezvousConsistentHashFactory.class,
            status.getJoinInfo().getConsistentHashFactory(),
            "After HHRCHF member joins, stored factory must be upgraded to HHRCHF");

      // And because the cluster is all-16.3, the fallback must NOT be active
      assertFalse(status.isUsingFallbackFactory(),
            "No fallback should be active in all-16.3 cluster with HHRCHF joinInfo");
   }

   // ---- Helpers ----

   private CacheJoinInfo joinInfoFor(Address a) {
      return new CacheJoinInfo(
            HHRCHF_JOIN_INFO.getConsistentHashFactory(),
            HHRCHF_JOIN_INFO.getNumSegments(),
            HHRCHF_JOIN_INFO.getNumOwners(),
            HHRCHF_JOIN_INFO.getTimeout(),
            HHRCHF_JOIN_INFO.getCacheMode(),
            HHRCHF_JOIN_INFO.getCapacityFactor(),
            new UUID(a.hashCode(), a.hashCode()),
            Optional.empty());
   }
}
