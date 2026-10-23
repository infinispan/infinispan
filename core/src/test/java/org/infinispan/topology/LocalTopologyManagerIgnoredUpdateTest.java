package org.infinispan.topology;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.TestModuleRepository;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.MockTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.skip.StringLogAppender;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.TestingEventLogManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests that {@link LocalTopologyManagerImpl} logs at {@code INFO} the topology updates it drops because the sender is
 * no longer the coordinator, or because they belong to a view older than the latest status request.
 *
 * <p>The other {@code Ignoring ...} messages in the same class are expected during a normal join and must stay below
 * {@code INFO}.</p>
 *
 * @since 16.0
 */
@Test(groups = "unit", testName = "topology.LocalTopologyManagerIgnoredUpdateTest")
public class LocalTopologyManagerIgnoredUpdateTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "testCache";
   private static final String OLD_COORDINATOR_MESSAGE_ID = "ISPN000731";
   private static final String OLD_VIEW_MESSAGE_ID = "ISPN000732";

   private static final Address A = Address.random("A");
   private static final Address B = Address.random("B");

   private final ExecutorService executor = Executors.newFixedThreadPool(2, getTestThreadFactory("Executor"));
   private final ScheduledExecutorService scheduledExecutor =
         Executors.newSingleThreadScheduledExecutor(getTestThreadFactory("Scheduled"));

   private final ConsistentHashFactory<?> replicatedChf = ReplicatedConsistentHashFactory.getInstance();

   @AfterClass(alwaysRun = true)
   public void tearDown() {
      executor.shutdownNow();
      scheduledExecutor.shutdownNow();
   }

   /**
    * A topology update sent by a node that is no longer the coordinator is dropped and logged at {@code INFO}.
    */
   public void testTopologyUpdateFromOldCoordinator() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         // A left the cluster, B is the new coordinator, but a topology update from A is still in flight
         fixture.transport.updateView(3, singletonList(B));

         CompletionStages.join(fixture.ltm.handleTopologyUpdate(CACHE_NAME, fixture.topology(5),
                                                                AvailabilityMode.AVAILABLE, 3, A));

         assertNull(fixture.cacheStatus.getCurrentTopology());
         assertNull(fixture.handler.lastTopology);
         fixture.assertLogged(OLD_COORDINATOR_MESSAGE_ID, "Ignoring topology 5 for cache " + CACHE_NAME +
                                                          " from old coordinator " + A);
         fixture.assertNotLogged(OLD_VIEW_MESSAGE_ID);
      } finally {
         fixture.close();
      }
   }

   /**
    * A rebalance start sent by a node that is no longer the coordinator is dropped and logged at {@code INFO}.
    */
   public void testRebalanceFromOldCoordinator() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         fixture.transport.updateView(3, singletonList(B));

         CompletionStages.join(fixture.ltm.handleRebalance(CACHE_NAME, fixture.rebalanceTopology(5),
                                                           AvailabilityMode.AVAILABLE, 3, A));

         assertNull(fixture.cacheStatus.getCurrentTopology());
         assertNull(fixture.handler.lastTopology);
         fixture.assertLogged(OLD_COORDINATOR_MESSAGE_ID, "Ignoring topology 5 for cache " + CACHE_NAME +
                                                          " from old coordinator " + A);
      } finally {
         fixture.close();
      }
   }

   /**
    * A stable topology update sent by a node that is no longer the coordinator is dropped and logged at {@code INFO}.
    */
   public void testStableTopologyUpdateFromOldCoordinator() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         fixture.transport.updateView(3, singletonList(B));

         CompletionStages.join(fixture.ltm.handleStableTopologyUpdate(CACHE_NAME, fixture.topology(5), A, 3));

         assertNull(fixture.cacheStatus.getStableTopology());
         fixture.assertLogged(OLD_COORDINATOR_MESSAGE_ID, "Ignoring topology 5 for cache " + CACHE_NAME +
                                                          " from old coordinator " + A);
      } finally {
         fixture.close();
      }
   }

   /**
    * After replying to a status request for a newer view, an update from an older view is dropped and logged at
    * {@code INFO}, even though the sender is still the coordinator (e.g. after a merge).
    */
   public void testTopologyUpdateFromViewOlderThanStatusRequest() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         // The coordinator recovers the cluster status for view 3, A is still the coordinator
         CompletionStages.join(fixture.ltm.handleStatusRequest(3));

         CompletionStages.join(fixture.ltm.handleTopologyUpdate(CACHE_NAME, fixture.topology(5),
                                                                AvailabilityMode.AVAILABLE, 2, A));

         assertNull(fixture.cacheStatus.getCurrentTopology());
         assertNull(fixture.handler.lastTopology);
         fixture.assertLogged(OLD_VIEW_MESSAGE_ID, "Ignoring topology 5 for cache " + CACHE_NAME +
                                                   " from view 2 received after status request from view 3");
         fixture.assertNotLogged(OLD_COORDINATOR_MESSAGE_ID);
      } finally {
         fixture.close();
      }
   }

   /**
    * Same as above, for a stable topology update.
    */
   public void testStableTopologyUpdateFromViewOlderThanStatusRequest() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         CompletionStages.join(fixture.ltm.handleStatusRequest(3));

         CompletionStages.join(fixture.ltm.handleStableTopologyUpdate(CACHE_NAME, fixture.topology(5), A, 2));

         assertNull(fixture.cacheStatus.getStableTopology());
         fixture.assertLogged(OLD_VIEW_MESSAGE_ID, "Ignoring topology 5 for cache " + CACHE_NAME +
                                                   " from view 2 received after status request from view 3");
      } finally {
         fixture.close();
      }
   }

   /**
    * An update from the current coordinator in the current view is applied and logs nothing at {@code INFO}.
    */
   public void testTopologyUpdateFromCurrentCoordinatorIsApplied() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         CacheTopology topology = fixture.topology(5);
         CompletionStages.join(fixture.ltm.handleTopologyUpdate(CACHE_NAME, topology, AvailabilityMode.AVAILABLE, 2, A));

         assertEquals(5, fixture.cacheStatus.getCurrentTopology().getTopologyId());
         assertEquals(5, fixture.handler.lastTopology.getTopologyId());
         fixture.assertNotLogged(OLD_COORDINATOR_MESSAGE_ID);
         fixture.assertNotLogged(OLD_VIEW_MESSAGE_ID);
      } finally {
         fixture.close();
      }
   }

   /**
    * A late update that the node has already superseded is expected on a joiner, so it must not be logged at
    * {@code INFO}.
    */
   public void testLateUpdatesAreNotLoggedAtInfo() throws Exception {
      Fixture fixture = new Fixture(2, asList(A, B));
      try {
         CompletionStages.join(fixture.ltm.handleTopologyUpdate(CACHE_NAME, fixture.topology(5),
                                                                AvailabilityMode.AVAILABLE, 2, A));
         assertEquals(5, fixture.cacheStatus.getCurrentTopology().getTopologyId());

         // Both the late consistent hash update and the old rebalance are dropped before validateCommandViewId()
         CompletionStages.join(fixture.ltm.handleTopologyUpdate(CACHE_NAME, fixture.topology(4),
                                                                AvailabilityMode.AVAILABLE, 2, A));
         CompletionStages.join(fixture.ltm.handleRebalance(CACHE_NAME, fixture.rebalanceTopology(4),
                                                           AvailabilityMode.AVAILABLE, 2, A));

         assertEquals(5, fixture.cacheStatus.getCurrentTopology().getTopologyId());
         fixture.assertNotLogged(OLD_COORDINATOR_MESSAGE_ID);
         fixture.assertNotLogged(OLD_VIEW_MESSAGE_ID);
      } finally {
         fixture.close();
      }
   }

   /**
    * Wires a real {@link LocalTopologyManagerImpl} with a {@link MockTransport} and captures the
    * {@code org.infinispan.CLUSTER} messages logged at {@code INFO} or above.
    */
   private class Fixture implements AutoCloseable {
      final MockTransport transport;
      final LocalTopologyManagerImpl ltm;
      final LocalCacheStatus cacheStatus;
      final RecordingCacheTopologyHandler handler = new RecordingCacheTopologyHandler();
      final StringLogAppender logAppender;

      private final GlobalComponentRegistry gcr;
      private final CacheJoinInfo joinInfo = new CacheJoinInfo(replicatedChf, 16, 1, 1000, CacheMode.REPL_SYNC,
                                                               1.0f, UUID.randomUUID(), Optional.empty());

      @SuppressWarnings("unchecked")
      Fixture(int viewId, List<Address> members) {
         logAppender = new StringLogAppender("org.infinispan.CLUSTER", Level.INFO, t -> true,
                                             PatternLayout.newBuilder().setPattern("%m").build());
         logAppender.install();

         GlobalConfiguration gc = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
         gcr = new GlobalComponentRegistry(gc, mock(EmbeddedCacheManager.class), Collections.emptySet(),
                                           TestModuleRepository.defaultModuleRepository(),
                                           mock(ConfigurationManager.class));
         BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);
         gbcr.replaceComponent(EventLogManager.class.getName(), new TestingEventLogManager(), false);

         CacheManagerNotifierImpl managerNotifier = new CacheManagerNotifierImpl();
         gbcr.replaceComponent(CacheManagerNotifier.class.getName(), managerNotifier, false);
         managerNotifier.start();

         // The local node is B, the coordinator is the first member of the view
         transport = new MockTransport(B);
         transport.init(viewId, members);
         gbcr.replaceComponent(Transport.class.getName(), transport, false);

         gbcr.replaceComponent(PersistentUUIDManager.class.getName(), new PersistentUUIDManagerImpl(), false);
         gbcr.replaceComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, executor, false);
         gbcr.replaceComponent(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, scheduledExecutor, false);

         GlobalStateManager globalStateManager = mock(GlobalStateManager.class);
         when(globalStateManager.readScopedState(any())).thenReturn(Optional.empty());
         gbcr.replaceComponent(GlobalStateManager.class.getName(), globalStateManager, false);

         ClusterTopologyManager ctm = mock(ClusterTopologyManager.class);
         when(ctm.isRebalancingEnabled()).thenReturn(true);
         gbcr.replaceComponent(ClusterTopologyManager.class.getName(), ctm, false);

         ltm = new LocalTopologyManagerImpl();
         gbcr.replaceComponent(LocalTopologyManager.class.getName(), ltm, false);
         gcr.rewire();
         ltm.preStart();
         ltm.start();

         PartitionHandlingManager phm = mock(PartitionHandlingManager.class);
         when(phm.getAvailabilityMode()).thenReturn(AvailabilityMode.AVAILABLE);
         when(phm.setAvailabilityMode(any())).thenReturn(CompletableFutures.completedNull());

         // A stableMembersSize of -1 means the cache does not need recovery, i.e. a plain join
         cacheStatus = new LocalCacheStatus(joinInfo, handler, phm, -1, null);
         Map<String, LocalCacheStatus> runningCaches = TestingUtil.extractField(ltm, "runningCaches");
         runningCaches.put(CACHE_NAME, cacheStatus);
      }

      CacheTopology topology(int topologyId) {
         ConsistentHash ch = replicatedChf.create(joinInfo.getNumOwners(), joinInfo.getNumSegments(), asList(A, B),
                                                  null);
         return new CacheTopology(topologyId, 1, ch, null, CacheTopology.Phase.NO_REBALANCE, asList(A, B),
                                  asList(UUID.randomUUID(), UUID.randomUUID()));
      }

      CacheTopology rebalanceTopology(int topologyId) {
         ConsistentHash currentCH = replicatedChf.create(joinInfo.getNumOwners(), joinInfo.getNumSegments(),
                                                         singletonList(A), null);
         ConsistentHash pendingCH = replicatedChf.create(joinInfo.getNumOwners(), joinInfo.getNumSegments(),
                                                         asList(A, B), null);
         return new CacheTopology(topologyId, 2, currentCH, pendingCH, CacheTopology.Phase.READ_OLD_WRITE_ALL,
                                  asList(A, B), asList(UUID.randomUUID(), UUID.randomUUID()));
      }

      void assertLogged(String messageId, String expectedMessage) {
         assertTrue(logged(messageId).anyMatch(s -> s.contains(expectedMessage)),
                    () -> "Expected " + messageId + " with '" + expectedMessage + "', logged: " + logged(messageId)
                          .toList());
      }

      void assertNotLogged(String messageId) {
         assertTrue(logged(messageId).findAny().isEmpty(),
                    () -> "Expected no " + messageId + " message, logged: " + logged(messageId).toList());
      }

      private Stream<String> logged(String messageId) {
         return StreamSupport.stream(logAppender.spliterator(), false).filter(s -> s.contains(messageId));
      }

      @Override
      public void close() {
         logAppender.uninstall();
         gcr.stop();
      }
   }

   private static class RecordingCacheTopologyHandler implements CacheTopologyHandler {
      volatile CacheTopology lastTopology;

      @Override
      public CompletionStage<Void> updateConsistentHash(CacheTopology cacheTopology) {
         lastTopology = cacheTopology;
         return CompletableFutures.completedNull();
      }

      @Override
      public CompletionStage<Void> rebalance(CacheTopology cacheTopology) {
         lastTopology = cacheTopology;
         return CompletableFutures.completedNull();
      }

      @Override
      public void onTopologyReceived(CacheTopology cacheTopology) {
         // no-op
      }
   }
}
