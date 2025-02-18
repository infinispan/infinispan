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

import org.infinispan.commands.topology.CacheStatusRequestCommand;
import org.infinispan.commands.topology.RebalanceStartCommand;
import org.infinispan.commands.topology.RebalanceStatusRequestCommand;
import org.infinispan.commands.topology.TopologyUpdateCommand;
import org.infinispan.commands.topology.TopologyUpdateStableCommand;
import org.infinispan.configuration.ConfigurationManager;
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
import org.infinispan.manager.TestModuleRepository;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.MockTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.TestingEventLogManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "topology.ClusterTopologyManagerImplTest")
public class ClusterTopologyManagerImplTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "testCache";

   private final ExecutorService executor = Executors.newFixedThreadPool(2, getTestThreadFactory("Executor"));
   private final ExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(getTestThreadFactory("Executor"));

   private static final Address A = new TestAddress(0, "A");
   private static final Address B = new TestAddress(1, "B");
   private final ConsistentHashFactory<?> replicatedChf = new ReplicatedConsistentHashFactory();
   // The persistent UUIDs are different, the rest of the join info is the same
   private final CacheJoinInfo joinInfoA = makeJoinInfo();
   private final CacheJoinInfo joinInfoB = makeJoinInfo();

   private CacheJoinInfo makeJoinInfo() {
      return new CacheJoinInfo(replicatedChf, 16, 1, 1000,
            CacheMode.REPL_SYNC, 1.0f, PersistentUUID.randomUUID(), Optional.empty());
   }

   /**
    * Start two nodes and make both join the cache.
    */
   public void testClusterStartupWith2Nodes() throws Exception {
      // Create global component registry with dependencies
      GlobalConfiguration gc = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cacheManager, Collections.emptySet(),
                                                                TestModuleRepository.defaultModuleRepository(),
                                                                mock(ConfigurationManager.class));
      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);
      gbcr.replaceComponent(EventLogManager.class.getName(), new TestingEventLogManager(), false);

      CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
      gbcr.replaceComponent(CacheManagerNotifier.class.getName(), managerNotifier, false);
      managerNotifier.start();

      MockTransport transport = new MockTransport(A);
      gbcr.replaceComponent(Transport.class.getName(), transport, false);

      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      gbcr.replaceComponent(PersistentUUIDManager.class.getName(), persistentUUIDManager, false);

      gbcr.replaceComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, executor, false);
      gbcr.replaceComponent(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, scheduledExecutor, false);

      MockLocalTopologyManager ltm = new MockLocalTopologyManager(CACHE_NAME);
      gbcr.replaceComponent(LocalTopologyManager.class.getName(), ltm, false);

      // Initial conditions
      transport.init(1, singletonList(A));
      ltm.init(null, null, null, null);

      // Component under test: ClusterTopologyManagerImpl on the coordinator (A)
      ClusterTopologyManagerImpl ctm = new ClusterTopologyManagerImpl();
      gbcr.replaceComponent(ClusterTopologyManager.class.getName(), ctm, false);
      gcr.rewire();
      ctm.start();

      // CTMI becomes coordinator and fetches the cluster status
      transport.expectCommand(CacheStatusRequestCommand.class).finish();

      // No caches, so no topology update is expected here
      Thread.sleep(1);
      transport.verifyNoErrors();

      // First node joins the cache
      CacheStatusResponse joinResponseA = CompletionStages.join(ctm.handleJoin(CACHE_NAME, A, joinInfoA, 1));
      assertEquals(1, joinResponseA.getCacheTopology().getTopologyId());
      assertCHMembers(joinResponseA.getCacheTopology().getCurrentCH(), A);
      assertNull(joinResponseA.getCacheTopology().getPendingCH());

      // LTMI normally updates the topology when receiving the join response
      ltm.handleTopologyUpdate(CACHE_NAME, joinResponseA.getCacheTopology(), joinResponseA.getAvailabilityMode(), 1, A);
      ltm.expectTopology(1, singletonList(A), null, CacheTopology.Phase.NO_REBALANCE);

      // CTMI replies to the initial stable topology broadcast
      transport.expectCommand(TopologyUpdateStableCommand.class, c -> {
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      }).finish();

      // Add a second node
      transport.updateView(2, asList(A, B));
      managerNotifier.notifyViewChange(asList(A, B), singletonList(A), A, 2);

      // CTMI confirms availability
      transport.expectHeartBeatCommand().finish();

      // Second node tries to join with old view and is rejected
      CacheStatusResponse joinResponseB1 = CompletionStages.join(ctm.handleJoin(CACHE_NAME, B, joinInfoB, 1));
      assertNull(joinResponseB1);

      // Second node joins the cache with correct view id, receives the initial topology
      CacheStatusResponse joinResponseB = CompletionStages.join(ctm.handleJoin(CACHE_NAME, B, joinInfoB, 2));
      assertEquals(1, joinResponseB.getCacheTopology().getTopologyId());
      assertCHMembers(joinResponseB.getCacheTopology().getCurrentCH(), A);
      assertNull(joinResponseB.getCacheTopology().getPendingCH());

      verifyRebalance(transport, ltm, ctm, 2, 1, singletonList(A), asList(A, B));

      transport.verifyNoErrors();
      gcr.stop();
   }

   /**
    * Assume there are already 2 nodes and the coordinator leaves during rebalance
    */
   public void testCoordinatorLostDuringRebalance() throws Exception {
      // Create global component registry with dependencies
      GlobalConfiguration gc = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cacheManager, Collections.emptySet(),
                                                                TestModuleRepository.defaultModuleRepository(),
                                                                mock(ConfigurationManager.class));
      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);
      gbcr.replaceComponent(EventLogManager.class.getName(), new TestingEventLogManager(), false);

      CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
      gbcr.replaceComponent(CacheManagerNotifier.class.getName(), managerNotifier, false);
      managerNotifier.start();

      MockTransport transport = new MockTransport(B);
      gbcr.replaceComponent(Transport.class.getName(), transport, false);

      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      gbcr.replaceComponent(PersistentUUIDManager.class.getName(), persistentUUIDManager, false);

      gbcr.replaceComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, executor, false);
      gbcr.replaceComponent(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, scheduledExecutor, false);

      MockLocalTopologyManager ltm = new MockLocalTopologyManager(CACHE_NAME);
      gbcr.replaceComponent(LocalTopologyManager.class.getName(), ltm, false);

      // Initial conditions (rebalance in phase 3, READ_NEW_WRITE_ALL)
      transport.init(2, asList(A, B));
      ConsistentHash stableCH = replicatedChf.create(joinInfoA.getNumOwners(),
                                                     joinInfoA.getNumSegments(), singletonList(A), null);
      ConsistentHash pendingCH = replicatedChf.create(joinInfoA.getNumOwners(),
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
      gcr.rewire();

      // When CTMI starts as regular member it requests the rebalancing status from the coordinator
      runConcurrently(
            ctm::start,
            () -> transport.expectCommand(RebalanceStatusRequestCommand.class)
                  .singleResponse(A, SuccessfulResponse.create(true)));

      // Wait for the initial view update in CTMI to finish
      eventuallyEquals(ClusterTopologyManager.ClusterManagerStatus.REGULAR_MEMBER, ctm::getStatus);

      // The coordinator (node A) leaves the cluster
      transport.updateView(3, singletonList(B));
      managerNotifier.notifyViewChange(singletonList(B), asList(A, B), B, 3);

      // Node B becomes coordinator and CTMI tries to recover the cluster status
      transport.expectCommand(CacheStatusRequestCommand.class).finish();

      // CTMI gets a single cache topology with READ_NEW and broadcasts a new topology with only the read CH
      ltm.expectTopology(5, asList(A, B), null, CacheTopology.Phase.NO_REBALANCE);
      transport.expectCommand(TopologyUpdateCommand.class, c -> {
         assertEquals(5, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), A, B);
         assertNull(c.getPendingCH());
      });
      transport.expectCommand(TopologyUpdateStableCommand.class, c -> {
         assertEquals(1, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), A);
         assertNull(c.getPendingCH());
      });

      // CTMI broadcasts a new cache topology with only node B
      ltm.expectTopology(6, singletonList(B), null, CacheTopology.Phase.NO_REBALANCE);
      transport.expectCommand(TopologyUpdateCommand.class, c -> {
         assertEquals(6, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), B);
         assertNull(c.getPendingCH());
      });

      // The new topology doesn't need rebalancing, so CTMI updates the stable topology
      transport.expectCommand(TopologyUpdateStableCommand.class, c -> {
         assertEquals(6, c.getTopologyId());
         assertCHMembers(c.getCurrentCH(), B);
         assertNull(c.getPendingCH());
      });

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
      gcr.stop();
   }

   private void verifyRebalance(MockTransport transport, MockLocalTopologyManager ltm, ClusterTopologyManagerImpl ctm,
                                int rebalanceTopologyId, int rebalanceViewId, List<Address> initialMembers,
                                List<Address> finalMembers) throws Exception {
      // CTMI starts rebalance
      ltm.expectTopology(rebalanceTopologyId, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_OLD_WRITE_ALL);
      transport.expectCommand(RebalanceStartCommand.class, c -> {
         assertEquals(rebalanceTopologyId, c.getTopologyId());
         assertEquals(CacheTopology.Phase.READ_OLD_WRITE_ALL, c.getPhase());
         assertEquals(initialMembers, c.getCurrentCH().getMembers());
         assertEquals(finalMembers, c.getPendingCH().getMembers());
      }).finish();

      // Confirm state transfer (phase 1, READ_OLD_WRITE_ALL)
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, A, rebalanceTopologyId, null);
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, B, rebalanceTopologyId, null);

      // CTMI starts phase 2, READ_ALL_WRITE_ALL
      ltm.expectTopology(rebalanceTopologyId + 1, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_ALL_WRITE_ALL);
      transport.expectCommand(TopologyUpdateCommand.class, c -> {
         assertEquals(rebalanceTopologyId + 1, c.getTopologyId());
         assertEquals(CacheTopology.Phase.READ_ALL_WRITE_ALL, c.getPhase());
         assertEquals(initialMembers, c.getCurrentCH().getMembers());
         assertEquals(finalMembers, c.getPendingCH().getMembers());
      }).finish();

      // Confirm phase 2
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, A, rebalanceTopologyId + 1, null);
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, B, rebalanceTopologyId + 1, null);

      // CTMI starts phase 3: READ_NEW_WRITE_ALL
      ltm.expectTopology(rebalanceTopologyId + 2, initialMembers, finalMembers,
                         CacheTopology.Phase.READ_NEW_WRITE_ALL);
      transport.expectCommand(TopologyUpdateCommand.class, c -> {
         assertEquals(rebalanceTopologyId + 2, c.getTopologyId());
         assertEquals(CacheTopology.Phase.READ_NEW_WRITE_ALL, c.getPhase());
         assertEquals(initialMembers, c.getCurrentCH().getMembers());
         assertEquals(finalMembers, c.getPendingCH().getMembers());
      }).finish();

      // Confirm phase 3
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, A, rebalanceTopologyId + 2, null);
      ctm.handleRebalancePhaseConfirm(CACHE_NAME, B, rebalanceTopologyId + 2, null);

      // CTMI finishes rebalance
      ltm.expectTopology(rebalanceTopologyId + 3, finalMembers, null, CacheTopology.Phase.NO_REBALANCE);
      transport.expectCommand(TopologyUpdateCommand.class, c -> {
         assertEquals(rebalanceTopologyId + 3, c.getTopologyId());
         assertEquals(CacheTopology.Phase.NO_REBALANCE, c.getPhase());
         assertEquals(finalMembers, c.getCurrentCH().getMembers());
         assertNull(c.getPendingCH());
      }).finish();

      transport.expectCommand(TopologyUpdateStableCommand.class, c -> {
         assertEquals(rebalanceTopologyId + 3, c.getTopologyId());
         assertEquals(finalMembers, c.getCurrentCH().getMembers());
         assertNull(c.getPendingCH());
      }).finish();
   }

   private void assertCHMembers(ConsistentHash ch, Address... members) {
      assertEquals(asList(members), ch.getMembers());
   }

   @AfterClass(alwaysRun = true)
   public void shutdownExecutors() throws InterruptedException {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      scheduledExecutor.shutdownNow();
      assertTrue(scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS));
   }
}
