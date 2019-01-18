package org.infinispan.topology;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.MockTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "topology.ClusterTopologyManagerImplTest")
public class ClusterTopologyManagerImplTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "testCache";

   private ExecutorService executor = Executors.newFixedThreadPool(2, getTestThreadFactory("Executor"));

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
      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);

      CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
      gbcr.replaceComponent(CacheManagerNotifier.class.getName(), managerNotifier, false);
      managerNotifier.start();

      MockTransport transport = new MockTransport(A);
      gbcr.replaceComponent(Transport.class.getName(), transport, false);

      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      gbcr.replaceComponent(PersistentUUIDManager.class.getName(), persistentUUIDManager, false);

      gbcr.replaceComponent(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR, executor, false);
      gbcr.replaceComponent(KnownComponentNames.STATE_TRANSFER_EXECUTOR, executor, false);

      MockLocalTopologyManager ltm = new MockLocalTopologyManager(CACHE_NAME);
      gbcr.replaceComponent(LocalTopologyManager.class.getName(), ltm, false);

      // Initial conditions
      transport.init(1, singletonList(A));
      ltm.init(null, null, null, null);

      // Component under test: ClusterTopologyManagerImpl on the coordinator (A)
      ClusterTopologyManagerImpl ctm = new ClusterTopologyManagerImpl();
      gbcr.replaceComponent(ClusterTopologyManager.class.getName(), ctm, false);

      ctm.start();

      // CTMI becomes coordinator and fetches the cluster status
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.GET_STATUS).finish();

      // CTMI fetches rebalance status/confirm members are available
      transport.expectHeartBeatCommand().finish();

      // First node joins the cache
      CacheStatusResponse joinResponseA = ctm.handleJoin(CACHE_NAME, A, joinInfoA, 1);
      assertEquals(1, joinResponseA.getCacheTopology().getTopologyId());
      assertCHMembers(joinResponseA.getCacheTopology().getCurrentCH(), A);
      assertNull(joinResponseA.getCacheTopology().getPendingCH());

      // LTMI normally updates the topology when receiving the join response
      ltm.handleTopologyUpdate(CACHE_NAME, joinResponseA.getCacheTopology(), joinResponseA.getAvailabilityMode(), 1, A);
      ltm.expectTopology(1, singletonList(A), null, CacheTopology.Phase.NO_REBALANCE);

      // CTMI replies to the initial stable topology broadcast
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      }).finish();

      // Add a second node
      transport.updateView(2, asList(A, B));
      managerNotifier.notifyViewChange(asList(A, B), singletonList(A), A, 2);

      // CTMI confirms availability
      transport.expectHeartBeatCommand().finish();

      // Second node joins the cache, receives the initial topology
      CacheStatusResponse joinResponseB = ctm.handleJoin(CACHE_NAME, B, joinInfoB, 2);
      assertEquals(1, joinResponseB.getCacheTopology().getTopologyId());
      assertCHMembers(joinResponseB.getCacheTopology().getCurrentCH(), A);
      assertNull(joinResponseB.getCacheTopology().getPendingCH());

      verifyRebalance(transport, ltm, ctm, 2, 1, singletonList(A), asList(A, B));

      transport.verifyNoErrors();
   }

   /**
    * Assume there are already 2 nodes and the coordinator leaves during rebalance
    */
   public void testCoordinatorLostDuringRebalance() throws Exception {
      // Create global component registry with dependencies
      GlobalConfiguration gc = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cacheManager, Collections.emptySet());
      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);

      CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
      gbcr.replaceComponent(CacheManagerNotifier.class.getName(), managerNotifier, false);
      managerNotifier.start();

      MockTransport transport = new MockTransport(B);
      gbcr.replaceComponent(Transport.class.getName(), transport, false);

      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      gbcr.replaceComponent(PersistentUUIDManager.class.getName(), persistentUUIDManager, false);

      gbcr.replaceComponent(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR, executor, false);
      gbcr.replaceComponent(KnownComponentNames.STATE_TRANSFER_EXECUTOR, executor, false);

      MockLocalTopologyManager ltm = new MockLocalTopologyManager(CACHE_NAME);
      gbcr.replaceComponent(LocalTopologyManager.class.getName(), ltm, false);

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
      gbcr.replaceComponent(ClusterTopologyManager.class.getName(), ctm, false);

      // When CTMI starts as regular member it requests the rebalancing status from the coordinator
      runConcurrently(
         ctm::start,
         () -> transport.expectTopologyCommand(CacheTopologyControlCommand.Type.POLICY_GET_STATUS)
                        .singleResponse(A, SuccessfulResponse.create(true)));

      // Wait for the initial view update in CTMI to finish
      eventuallyEquals(ClusterTopologyManager.ClusterManagerStatus.REGULAR_MEMBER, () -> ctm.getStatus());

      // The coordinator (node A) leaves the cluster
      transport.updateView(3, singletonList(B));
      managerNotifier.notifyViewChange(singletonList(B), asList(A, B), B, 3);

      // Node B becomes coordinator and CTMI tries to recover the cluster status
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.GET_STATUS).finish();

      // CTMI gets a single cache topology with READ_NEW and broadcasts a new topology with only the read CH
      ltm.expectTopology(5, asList(A, B), null, CacheTopology.Phase.NO_REBALANCE);
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(5, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), A, B);
         assertNull(c.getPendingCH());
      });
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertEquals(1, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      });

      // CTMI broadcasts a new cache topology with only node B
      ltm.expectTopology(6, singletonList(B), null, CacheTopology.Phase.NO_REBALANCE);
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(6, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), B);
         assertNull(c.getPendingCH());
      });

      // The new topology doesn't need rebalancing, so CTMI updates the stable topology
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertEquals(6, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), B);
         assertNull(c.getPendingCH());
      });

      // CTMI confirms members are available in case it needs to starts a rebalance
      transport.expectHeartBeatCommand().finish();

      // Shouldn't send any more commands here
      Thread.sleep(1);
      transport.verifyNoErrors();

      // Node A restarts
      transport.updateView(4, asList(B, A));
      managerNotifier.notifyViewChange(asList(B, A), singletonList(B), A, 4);

      // CTMI confirms members are available in case it needs to starts a rebalance
      transport.expectHeartBeatCommand().finish();

      // Node A rejoins
      ctm.handleJoin(CACHE_NAME, A, joinInfoA, 4);

      verifyRebalance(transport, ltm, ctm, 7, 4, singletonList(B), asList(B, A));

      transport.verifyNoErrors();
   }

   private void verifyRebalance(MockTransport transport, MockLocalTopologyManager ltm, ClusterTopologyManagerImpl ctm,
                                int rebalanceTopologyId, int rebalanceViewId, List<Address> initialMembers,
                                List<Address> finalMembers) throws Exception {
      // CTMI starts rebalance
      ltm.expectTopology(rebalanceTopologyId, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_OLD_WRITE_ALL);
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.REBALANCE_START, c -> {
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
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
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
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
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
      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.CH_UPDATE, c -> {
         assertEquals(rebalanceTopologyId + 3, c.getTopologyId());
         assertEquals(CacheTopology.Phase.NO_REBALANCE, c.getPhase());
         assertEquals(finalMembers, c.getCurrentCH().getMembers());
         assertNull(c.getPendingCH());
      }).finish();

      transport.expectTopologyCommand(CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, c -> {
         assertEquals(rebalanceTopologyId + 3, c.getTopologyId());
         assertEquals(CacheTopology.Phase.NO_REBALANCE, c.getPhase());
         assertEquals(finalMembers, c.getCurrentCH().getMembers());
         assertNull(c.getPendingCH());
      }).finish();
   }

   private void assertCHMembers(ConsistentHash ch, Address... members) {
      assertEquals(asList(members), ch.getMembers());
   }

   @AfterClass(alwaysRun = true)
   public void shutdownExecutor() throws InterruptedException {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
   }
}
