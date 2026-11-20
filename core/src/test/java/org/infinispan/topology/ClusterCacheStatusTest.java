package org.infinispan.topology;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.remoting.transport.Address;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "topology.ClusterCacheStatusTest")
public class ClusterCacheStatusTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "test";
   private static final CacheJoinInfo JOIN_INFO =
      new CacheJoinInfo(DefaultConsistentHashFactory.getInstance(), 8, 2, 1000,
            CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());
   private static final Address A = Address.random("A");
   private static final Address B = Address.random("B");
   private static final Address C = Address.random("C");

   private ClusterCacheStatus status;
   private ClusterTopologyManagerImpl topologyManager;
   private MockitoSession mockitoSession;
   private Transport transport;
   private EventLogManager eventLogManager;
   private PersistentUUIDManager persistentUUIDManager;
   private EmbeddedCacheManager cacheManager;
   private StateTransferTracker stateTransferTracker;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession().strictness(Strictness.STRICT_STUBS).startMocking();

      eventLogManager = new TestingEventLogManager();
      persistentUUIDManager = new PersistentUUIDManagerImpl();
      cacheManager = mock(EmbeddedCacheManager.class);
      topologyManager = mock(ClusterTopologyManagerImpl.class);
      transport = mock(Transport.class);
      stateTransferTracker = mock(StateTransferTracker.class);
      when(stateTransferTracker.forCache(any())).thenReturn(mock(StateTransferTracker.CacheStateTransferTracker.class));
      status = createCacheStatus(Optional.empty());
   }

   /**
    * Creates the coordinator's view of {@link #CACHE_NAME}, sharing the mocks created by {@link #setup()}.
    *
    * @param state the persistent state the coordinator restarts from, or empty for a cache starting without state.
    * @return a new {@link ClusterCacheStatus} in the initial status.
    */
   private ClusterCacheStatus createCacheStatus(Optional<ScopedPersistentState> state) {
      PreferAvailabilityStrategy availabilityStrategy =
         new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager);
      return new ClusterCacheStatus(cacheManager, null, CACHE_NAME, availabilityStrategy, RebalanceType.FOUR_PHASE,
                                    topologyManager, transport, stateTransferTracker, persistentUUIDManager, eventLogManager,
                                    state, false);
   }

   @AfterMethod(alwaysRun = true)
   public void teardown() {
      mockitoSession.finishMocking();
   }

   @Test
   public void testQueueRebalanceSingleNode() throws Exception {
      when(topologyManager.isRebalancingEnabled()).thenReturn(true);

      status.doJoin(A, makeJoinInfo(A));
      verifyStableTopologyUpdate();

      status.doJoin(B, makeJoinInfo(B));
      verifyRebalanceStart();
      completeRebalance(status);
      verifyStableTopologyUpdate();

      status.doJoin(C, makeJoinInfo(C));
      verifyRebalanceStart();
      completeRebalance(status);
      verifyStableTopologyUpdate();

      when(transport.getMembers()).thenReturn(singletonList(C));
      when(transport.getViewId()).thenReturn(1);
      status.doHandleClusterView(1);

      TestClusterCacheStatus cache = TestClusterCacheStatus.start(JOIN_INFO, C);
      cache.incrementIds(9, 2);
      cache.incrementStableIds(9, 2);
      assertEquals(cache.topology(), status.getCurrentTopology());
      assertEquals(cache.stableTopology(), status.getStableTopology());
      verifyTopologyUpdate();
      verifyStableTopologyUpdate();

      verifyNoMoreInteractions(topologyManager);
   }

   @DataProvider
   public Object[][] recoveryCacheModes() {
      return new Object[][]{{CacheMode.DIST_SYNC}, {CacheMode.REPL_SYNC}};
   }

   /**
    * A node without state must not join while the members restored from persistent state have not yet confirmed the
    * stable topology. The coordinator rejects it with an empty response, and the joiner is responsible for retrying.
    * Once every member confirms, the retried join is accepted and starts a rebalance.
    */
   @Test(dataProvider = "recoveryCacheModes")
   public void testJoinerWithoutStateRetriesAfterStableTopologyConfirmation(CacheMode cacheMode) throws Exception {
      ConsistentHashFactory<?> factory = cacheMode.isDistributed()
            ? DefaultConsistentHashFactory.getInstance()
            : ReplicatedConsistentHashFactory.getInstance();

      // A and B were the members before the shutdown, so their CH is in the persistent state.
      ScopedPersistentStateImpl state = new ScopedPersistentStateImpl(CACHE_NAME);
      ConsistentHash ch = factory.create(2, 8, List.of(A, B), Map.of(A, 1.0f, B, 1.0f));
      ch.toScopedState(state, a -> makeJoinInfo(a).getPersistentUUID());
      status = createCacheStatus(Optional.of(state));
      when(topologyManager.isRebalancingEnabled()).thenReturn(true);

      for (Address member : List.of(A, B)) {
         status.doJoin(member, new CacheJoinInfo(factory, 8, 2, 1000, cacheMode, 1.0f,
               makeJoinInfo(member).getPersistentUUID(), Optional.of(state.getChecksum())));
      }
      CacheTopology restoredTopology = status.getCurrentTopology();
      assertTrue(restoredTopology.wasTopologyRestoredFromState());

      // C has no state. It joins while the stable topology confirmations from A and B are still in flight.
      CacheJoinInfo joinerWithoutState = new CacheJoinInfo(factory, 8, 2, 1000, cacheMode, 1.0f,
            makeJoinInfo(C).getPersistentUUID(), Optional.empty());
      assertTrue(status.doJoin(C, joinerWithoutState).isEmpty());
      assertFalse(status.getExpectedMembers().contains(C));
      assertEquals(restoredTopology, status.getCurrentTopology());

      // B has not confirmed yet, so the recovery is still in progress and C is rejected again.
      status.stableTopologyInstallConfirmation(
            new StableTopologyConfirmationCollector.StableTopologyConfirmationResult(Set.of(B)));
      assertTrue(status.doJoin(C, joinerWithoutState).isEmpty());
      assertFalse(status.getExpectedMembers().contains(C));
      assertEquals(restoredTopology, status.getCurrentTopology());

      // Every member confirmed: the stable topology is committed and C's retry is accepted.
      status.stableTopologyInstallConfirmation(
            new StableTopologyConfirmationCollector.StableTopologyConfirmationResult(Set.of()));
      assertFalse(status.getStableTopology().wasTopologyRestoredFromState());
      assertFalse(status.doJoin(C, joinerWithoutState).isEmpty());

      assertEquals(CacheTopology.Phase.READ_OLD_WRITE_ALL, status.getCurrentTopology().getPhase());
      assertEquals(List.of(A, B, C), status.getCurrentTopology().getPendingCH().getMembers());
      verifyRebalanceStart();
      completeRebalance(status);
      assertEquals(List.of(A, B, C), status.getCurrentTopology().getCurrentCH().getMembers());
   }

   private void verifyRebalanceStart() {
      verify(topologyManager).broadcastRebalanceStart(CACHE_NAME, status.getCurrentTopology(),
                                                      AvailabilityMode.AVAILABLE);
   }

   private void verifyStableTopologyUpdate() {
      verify(topologyManager).broadcastStableTopologyUpdate(CACHE_NAME, status.getStableTopology());
   }

   private void verifyTopologyUpdate() {
      verify(topologyManager).broadcastTopologyUpdate(CACHE_NAME, status.getCurrentTopology(),
                                                      AvailabilityMode.AVAILABLE);
   }

   private void completeRebalance(ClusterCacheStatus status) throws Exception {
      advanceRebalance(status, CacheTopology.Phase.READ_OLD_WRITE_ALL, CacheTopology.Phase.READ_ALL_WRITE_ALL,
                       CacheTopology.Phase.READ_NEW_WRITE_ALL, CacheTopology.Phase.NO_REBALANCE);
   }

   private void advanceRebalance(ClusterCacheStatus status, CacheTopology.Phase initialPhase,
                                 CacheTopology.Phase... phases) throws Exception {
      assertEquals(initialPhase, status.getCurrentTopology().getPhase());
      for (CacheTopology.Phase phase : phases) {
         confirmRebalancePhase(status, status.getCurrentTopology().getMembers());
         assertEquals(phase, status.getCurrentTopology().getPhase());
         verifyTopologyUpdate();
      }
   }

   private void confirmRebalancePhase(ClusterCacheStatus status, List<Address> members) throws Exception {
      int topologyId = status.getCurrentTopology().getTopologyId();
      for (Address a : members) {
         status.confirmRebalancePhase(a, topologyId);
      }
      assertEquals(topologyId + 1, status.getCurrentTopology().getTopologyId());
   }

   private CacheJoinInfo makeJoinInfo(Address a) {
      var persistentUUID = new UUID(a.hashCode(), a.hashCode());
      return new CacheJoinInfo(JOIN_INFO.getConsistentHashFactory(), JOIN_INFO.getNumSegments(), JOIN_INFO.getNumOwners(),
            JOIN_INFO.getTimeout(), JOIN_INFO.getCacheMode(), JOIN_INFO.getCapacityFactor(),
            persistentUUID, Optional.empty());
   }
}
