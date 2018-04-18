package org.infinispan.topology;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.Optional;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClusterCacheStatusTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "test";
   private static final CacheJoinInfo JOIN_INFO =
      new CacheJoinInfo(new DefaultConsistentHashFactory(), MurmurHash3.getInstance(), 8, 2, 1000, false,
                        CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());
   private static final Address A = new TestAddress(1, "A");
   private static final Address B = new TestAddress(2, "B");
   private static final Address C = new TestAddress(3, "C");

   private ClusterCacheStatus status;
   private ClusterTopologyManager topologyManager;
   private MockitoSession mockitoSession;
   private Transport transport;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession().strictness(Strictness.STRICT_STUBS).startMocking();

      EventLogManager eventLogManager = new EventLogManagerImpl();
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      topologyManager = mock(ClusterTopologyManager.class);
      transport = mock(Transport.class);
      PreferAvailabilityStrategy availabilityStrategy =
         new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager,
                                        ClusterTopologyManagerImpl::distLostDataCheck);
      status = new ClusterCacheStatus(cacheManager, CACHE_NAME, availabilityStrategy, RebalanceType.FOUR_PHASE,
                                      topologyManager, transport, persistentUUIDManager, eventLogManager,
                                      Optional.empty(), false);
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
      status.doHandleClusterView();

      TestClusterCacheStatus cache = TestClusterCacheStatus.start(JOIN_INFO, C);
      cache.incrementIds(9, 2);
      cache.incrementStableIds(9, 2);
      assertEquals(cache.topology(), status.getCurrentTopology());
      assertEquals(cache.stableTopology(), status.getStableTopology());
      verifyTopologyUpdate();
      verifyStableTopologyUpdate();

      verifyNoMoreInteractions(topologyManager);
   }

   private void verifyRebalanceStart() {
      verify(topologyManager).broadcastRebalanceStart(CACHE_NAME, status.getCurrentTopology(),
                                                      JOIN_INFO.isTotalOrder(),
                                                      JOIN_INFO.getCacheMode().isDistributed());
   }

   private void verifyStableTopologyUpdate() {
      verify(topologyManager).broadcastStableTopologyUpdate(CACHE_NAME, status.getStableTopology(),
                                                            JOIN_INFO.isTotalOrder(),
                                                            JOIN_INFO.getCacheMode().isDistributed());
   }

   private void verifyTopologyUpdate() {
      verify(topologyManager).broadcastTopologyUpdate(CACHE_NAME, status.getCurrentTopology(),
                                                      AvailabilityMode.AVAILABLE, JOIN_INFO.isTotalOrder(),
                                                      JOIN_INFO.getCacheMode().isDistributed());
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
      PersistentUUID persistentUUID = new PersistentUUID(a.hashCode(), a.hashCode());
      return new CacheJoinInfo(JOIN_INFO.getConsistentHashFactory(), JOIN_INFO.getHashFunction(),
                               JOIN_INFO.getNumSegments(), JOIN_INFO.getNumOwners(), JOIN_INFO.getTimeout(),
                               JOIN_INFO.isTotalOrder(), JOIN_INFO.getCacheMode(), JOIN_INFO.getCapacityFactor(),
                               persistentUUID, Optional.empty());
   }
}
