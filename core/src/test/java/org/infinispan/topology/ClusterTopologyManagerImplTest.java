package org.infinispan.topology;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.MockTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "topology.ClusterTopologyManagerImplTest")
public class ClusterTopologyManagerImplTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "testCache";

   private static final Address A = new TestAddress(0, "A");
   private static final Address B = new TestAddress(1, "B");
   private final ConsistentHashFactory replicatedChf = new ReplicatedConsistentHashFactory();
   // The persistent UUIDs are different, the rest of the join info is the same
   private final CacheJoinInfo joinInfoA = makeJoinInfo();
   private final CacheJoinInfo joinInfoB = makeJoinInfo();

   private CacheJoinInfo makeJoinInfo() {
      return new CacheJoinInfo(replicatedChf, MurmurHash3.getInstance(), 16, 1, 1000, false,
                               CacheMode.REPL_SYNC, 1.0f, PersistentUUID.randomUUID(), Optional.empty());
   }

   /**
    * Start two nodes and make both join the cache.
    */
   public void testClusterStartupWith2Nodes() throws Exception {
      // Create global component registry with dependencies
      GlobalConfiguration gc = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cacheManager, Collections.emptySet());

      CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
      gcr.registerComponent(managerNotifier, CacheManagerNotifier.class);
      managerNotifier.start();

      MockTransport transport = new MockTransport(A);
      gcr.registerComponent(transport, Transport.class);

      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      gcr.registerComponent(persistentUUIDManager, PersistentUUIDManager.class);

      ExecutorService transportExecutor = Executors.newSingleThreadExecutor(getTestThreadFactory("Transport"));
      gcr.registerComponent(transportExecutor, KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR);

      MockLocalTopologyManager ltm = new MockLocalTopologyManager(CACHE_NAME);
      gcr.registerComponent(ltm, LocalTopologyManager.class);

      // Initial conditions
      transport.init(1, singletonList(A));
      ltm.init(null, null, null, null);

      // Component under test: ClusterTopologyManagerImpl on the coordinator (A)
      ClusterTopologyManagerImpl ctm = new ClusterTopologyManagerImpl();
      gcr.registerComponent(ctm, ClusterTopologyManager.class);

      ctm.start();

      // CTMI becomes coordinator and fetches the cluster status
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.GET_STATUS).finish();

      // CTMI fetches rebalance status/confirm members are available
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS).finish();

      // First node joins the cache
      CacheStatusResponse joinResponseA = ctm.handleJoin(CACHE_NAME, A, joinInfoA, 1);
      assertEquals(1, joinResponseA.getCacheTopology().getTopologyId());
      assertCHMembers(joinResponseA.getCacheTopology().getCurrentCH(), A);
      assertNull(joinResponseA.getCacheTopology().getPendingCH());

      // LTMI normally updates the topology when receiving the join response
      ltm.handleTopologyUpdate(CACHE_NAME, joinResponseA.getCacheTopology(), joinResponseA.getAvailabilityMode(), 1, A);
      ltm.expectTopology(1, singletonList(A), null, CacheTopology.Phase.NO_REBALANCE);

      // CTMI replies to the initial stable topology broadcast
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      }).finish();

      // Add a second node
      transport.updateView(2, asList(A, B));
      managerNotifier.notifyViewChange(asList(A, B), singletonList(A), A, 2);

      // CTMI confirms availability
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS).finish();

      // Second node joins the cache, receives the initial topology
      CacheStatusResponse joinResponseB = ctm.handleJoin(CACHE_NAME, B, joinInfoB, 2);
      assertEquals(1, joinResponseB.getCacheTopology().getTopologyId());
      assertCHMembers(joinResponseB.getCacheTopology().getCurrentCH(), A);
      assertNull(joinResponseB.getCacheTopology().getPendingCH());

      verifyRebalance(transport, ltm, ctm, 2, 1, singletonList(A), asList(A, B));

      transport.verifyNoMoreCommands();
   }

   /**
    * Assume there are already 2 nodes and the coordinator leaves during rebalance
    */
   public void testCoordinatorLostDuringRebalance() throws Exception {
      // Create global component registry with dependencies
      GlobalConfiguration gc = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cacheManager, Collections.emptySet());

      CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
      gcr.registerComponent(managerNotifier, CacheManagerNotifier.class);
      managerNotifier.start();

      MockTransport transport = new MockTransport(B);
      gcr.registerComponent(transport, Transport.class);

      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      gcr.registerComponent(persistentUUIDManager, PersistentUUIDManager.class);

      ExecutorService transportExecutor = Executors.newFixedThreadPool(2, getTestThreadFactory("Transport"));
      gcr.registerComponent(transportExecutor, KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR);

      MockLocalTopologyManager ltm = new MockLocalTopologyManager(CACHE_NAME);
      gcr.registerComponent(ltm, LocalTopologyManager.class);

      // Initial conditions (rebalance in phase 3, READ_NEW_WRITE_ALL)
      transport.init(2, asList(A, B));
      ConsistentHash stableCH = replicatedChf.create(MurmurHash3.getInstance(), joinInfoA.getNumOwners(),
                                                     joinInfoA.getNumSegments(), singletonList(A), null);
      ConsistentHash pendingCH = replicatedChf.create(MurmurHash3.getInstance(), joinInfoA.getNumOwners(),
                                                      joinInfoA.getNumSegments(), asList(A, B), null);
      CacheTopology initialTopology = new CacheTopology(4, 2, stableCH, pendingCH,
                                                        CacheTopology.Phase.READ_NEW_WRITE_ALL, asList(A, B),
                                                        asList(joinInfoA.getPersistentUUID(),
                                                               joinInfoB.getPersistentUUID()));
      CacheTopology stableTopology = new CacheTopology(1, 1, stableCH, null,
                                                       CacheTopology.Phase.NO_REBALANCE, singletonList(A),
                                                       singletonList(joinInfoA.getPersistentUUID()));
      ltm.init(joinInfoA, initialTopology, stableTopology, AvailabilityMode.AVAILABLE);
      // Normally LocalTopologyManagerImpl.start()/doHandleTopologyUpdate() registers the persistent UUIDs
      // TODO Write test with asymmetric caches leaving the PersistentUUIDManager cache incomplete
      persistentUUIDManager.addPersistentAddressMapping(A, joinInfoA.getPersistentUUID());
      persistentUUIDManager.addPersistentAddressMapping(B, joinInfoB.getPersistentUUID());

      // Component under test: ClusterTopologyManagerImpl on the new coordinator (B)
      ClusterTopologyManagerImpl ctm = new ClusterTopologyManagerImpl();
      gcr.registerComponent(ctm, ClusterTopologyManager.class);

      // As CTMI starts as regular member it will request the rebalancing status from the coordinator
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS)
               .singleResponse(A, SuccessfulResponse.create(true));

      ctm.start();
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS).assertDone();

      // The coordinator (node A) leaves the cluster
      transport.updateView(3, singletonList(B));
      managerNotifier.notifyViewChange(singletonList(B), asList(A, B), B, 3);

      // Node B becomes coordinator and CTMI tries to recover the cluster status
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.GET_STATUS).finish();

      // CTMI broadcasts the only cache topology it got, but without the pending CH
      ltm.expectTopology(5, singletonList(A), null, CacheTopology.Phase.NO_REBALANCE);
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(5, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      });
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertEquals(1, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      });

      // CTMI broadcasts a new cache topology with only node B
      ltm.expectTopology(6, singletonList(B), null, CacheTopology.Phase.NO_REBALANCE);
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(6, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), B);
         assertNull(c.getPendingCH());
      });

      // CTMI confirms members are available in case it needs to starts a rebalance
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS).finish();

      // Node A restarts
      transport.updateView(4, asList(B, A));
      managerNotifier.notifyViewChange(asList(B, A), singletonList(B), A, 4);

      // CTMI confirms members are available in case it needs to starts a rebalance
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS)
               .singleResponse(A, SuccessfulResponse.create(true));

      // Node A rejoins
      ctm.handleJoin(CACHE_NAME, A, joinInfoA, 4);

      verifyRebalance(transport, ltm, ctm, 7, 4, singletonList(B), asList(B, A));

      transport.verifyNoMoreCommands();
   }

   private void verifyRebalance(MockTransport transport, MockLocalTopologyManager ltm, ClusterTopologyManagerImpl ctm,
                                int rebalanceTopologyId, int rebalanceViewId, List<Address> initialMembers,
                                List<Address> finalMembers) throws Exception {
      // CTMI starts rebalance
      ltm.expectTopology(rebalanceTopologyId, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_OLD_WRITE_ALL);
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.REBALANCE_START, c -> {
         assertEquals(rebalanceTopologyId, c.getTopologyId());
         assertEquals(CacheTopology.Phase.READ_OLD_WRITE_ALL, c.getPhase());
         assertEquals(initialMembers, c.getCurrentCH().getMembers());
         assertEquals(finalMembers, c.getPendingCH().getMembers());
      }).finish();

      // Confirm state transfer (phase 1, READ_OLD_WRITE_ALL)
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, A, rebalanceTopologyId, null, rebalanceViewId);
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, B, rebalanceTopologyId, null, rebalanceViewId);

      // CTMI starts phase 2, READ_ALL_WRITE_ALL
      ltm.expectTopology(rebalanceTopologyId + 1, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_ALL_WRITE_ALL);
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(rebalanceTopologyId + 1, c.getTopologyId());
         assertEquals(CacheTopology.Phase.READ_ALL_WRITE_ALL, c.getPhase());
         assertEquals(initialMembers, c.getCurrentCH().getMembers());
         assertEquals(finalMembers, c.getPendingCH().getMembers());
      }).finish();

      // Confirm phase 2
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, A, rebalanceTopologyId + 1, null, rebalanceViewId);
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, B, rebalanceTopologyId + 1, null, rebalanceViewId);

      // CTMI starts phase 3: READ_NEW_WRITE_ALL
      ltm.expectTopology(rebalanceTopologyId + 2, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_NEW_WRITE_ALL);
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(rebalanceTopologyId + 2, c.getTopologyId());
         assertEquals(CacheTopology.Phase.READ_NEW_WRITE_ALL, c.getPhase());
         assertEquals(initialMembers, c.getCurrentCH().getMembers());
         assertEquals(finalMembers, c.getPendingCH().getMembers());
      }).finish();

      // Confirm phase 3
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, A, rebalanceTopologyId + 2, null, rebalanceViewId);
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, B, rebalanceTopologyId + 2, null, rebalanceViewId);

      // CTMI finishes rebalance
      ltm.expectTopology(rebalanceTopologyId + 3, finalMembers, null, CacheTopology.Phase.NO_REBALANCE);
      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(rebalanceTopologyId + 3, c.getTopologyId());
         assertEquals(CacheTopology.Phase.NO_REBALANCE, c.getPhase());
         assertEquals(finalMembers, c.getCurrentCH().getMembers());
         assertNull(c.getPendingCH());
      }).finish();

      transport.verifyTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertEquals(rebalanceTopologyId + 3, c.getTopologyId());
         assertEquals(CacheTopology.Phase.NO_REBALANCE, c.getPhase());
         assertEquals(finalMembers, c.getCurrentCH().getMembers());
         assertNull(c.getPendingCH());
      }).finish();
   }

   private void assertCHMembers(ConsistentHash ch, Address... members) {
      assertEquals(asList(members), ch.getMembers());
   }

}
