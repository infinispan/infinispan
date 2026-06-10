package org.infinispan.partitionhandling;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.partitionhandling.AvailabilityMode.DEGRADED_MODE;
import static org.infinispan.test.TestingUtil.mapOf;
import static org.infinispan.topology.TestClusterCacheStatus.persistentUUID;
import static org.infinispan.topology.TestClusterCacheStatus.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.AvailabilityStrategyContext;
import org.infinispan.partitionhandling.impl.PreferConsistencyStrategy;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.statetransfer.StateTransferTracker;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.topology.RebalancingStatus;
import org.infinispan.topology.TestClusterCacheStatus;
import org.infinispan.util.logging.events.TestingEventLogManager;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "partitionhandling.PreferConsistencyStrategyTest")
public class PreferConsistencyStrategyTest extends AbstractInfinispanTest {

   private static final CacheJoinInfo DIST_INFO =
         new CacheJoinInfo(DefaultConsistentHashFactory.getInstance(), 8, 2, 1000, CacheMode.DIST_SYNC, 1.0f, null,
                           Optional.empty());
   private static final Address A = Address.random("A");
   private static final Address B = Address.random("B");
   private static final Address C = Address.random("C");
   private static final Address D = Address.random("D");
   private static final Address E = Address.random("E");
   private static final String CACHE_NAME = "test";

   private AvailabilityStrategyContext context;
   private PreferConsistencyStrategy strategy;
   private MockitoSession mockitoSession;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession()
                              .strictness(Strictness.WARN)
                              .startMocking();
      PersistentUUIDManagerImpl persistentUUIDManager = new PersistentUUIDManagerImpl();
      context = mock(AvailabilityStrategyContext.class);

      persistentUUIDManager.addPersistentAddressMapping(A, persistentUUID(A));
      persistentUUIDManager.addPersistentAddressMapping(B, persistentUUID(B));
      persistentUUIDManager.addPersistentAddressMapping(C, persistentUUID(C));
      persistentUUIDManager.addPersistentAddressMapping(D, persistentUUID(D));
      persistentUUIDManager.addPersistentAddressMapping(E, persistentUUID(E));

      strategy = new PreferConsistencyStrategy(new TestingEventLogManager(), persistentUUIDManager);
   }

   @AfterMethod(alwaysRun = true)
   public void teardown() {
      mockitoSession.finishMocking();
   }

   public void testAvoidingNullPointerExceptionWhenUpdatingPartitionWithNullTopology() {
      //when
      PersistentUUIDManagerImpl uuidManager = new PersistentUUIDManagerImpl();
      PreferConsistencyStrategy preferConsistencyStrategy = new PreferConsistencyStrategy(new TestingEventLogManager(), uuidManager);
      ClusterTopologyManagerImpl topologyManager = new ClusterTopologyManagerImpl();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      StateTransferTracker stateTransferTracker = mock(StateTransferTracker.class);
      ClusterCacheStatus status = new ClusterCacheStatus(cacheManager, null, "does-not-matter", preferConsistencyStrategy, RebalanceType.FOUR_PHASE, topologyManager,
            null, stateTransferTracker, uuidManager, new TestingEventLogManager(), Optional.empty(), false);

      preferConsistencyStrategy.onPartitionMerge(status, Collections.emptyMap());

      //then
      assertNull(status.getCurrentTopology());
      assertNull(status.getStableTopology());
      assertEquals(AvailabilityMode.AVAILABLE, status.getAvailabilityMode());
      assertEquals(RebalancingStatus.COMPLETE, status.getRebalancingStatus());
      assertThat(status.getCurrentTopology()).isNull();
      assertThat(status.getStableTopology()).isNull();
      assertThat(status.getAvailabilityMode()).isEqualTo(AvailabilityMode.AVAILABLE);
      assertThat(status.getRebalancingStatus()).isEqualTo(RebalancingStatus.COMPLETE);
   }

   public void testCoordinatorFailoverDoesNotTriggerConflictResolution() {
      TestClusterCacheStatus cacheABCD = start(DIST_INFO, A, B, C, D);
      List<Address> remainingMembers = asList(B, C, D);

      Map<Address, CacheStatusResponse> statusResponses = mapOf(
            B, availableResponse(B, cacheABCD),
            C, availableResponse(C, cacheABCD),
            D, availableResponse(D, cacheABCD));

      when(context.getCacheName()).thenReturn(CACHE_NAME);
      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(true);

      // Invoked by a coordinator failover by ClusterTopologyManager
      strategy.onPartitionMerge(context, statusResponses);

      // Assert that a coordinator change with consistency strategy doesn't trigger conflict resolution.
      verify(context, never()).queueConflictResolution(any(), any());
      verify(context).queueRebalance(remainingMembers);
   }

   public void testCoordinatorFailoverWithNewNodeJoinDoesNotTriggerConflictResolution() {
      TestClusterCacheStatus cacheABCD = start(DIST_INFO, A, B, C, D);
      List<Address> newMembers = asList(B, C, D, E);

      Map<Address, CacheStatusResponse> statusResponses = mapOf(
            B, availableResponse(B, cacheABCD),
            C, availableResponse(C, cacheABCD),
            D, availableResponse(D, cacheABCD),
            E, joinerResponse());

      when(context.getCacheName()).thenReturn(CACHE_NAME);
      when(context.getExpectedMembers()).thenReturn(newMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(true);

      strategy.onPartitionMerge(context, statusResponses);

      verify(context, never()).queueConflictResolution(any(), any());
      verify(context).queueRebalance(newMembers);
   }

   public void testRealMergeWithDegradedPartitionTriggersConflictResolution() {
      TestClusterCacheStatus cacheABCD = start(DIST_INFO, A, B, C, D);
      TestClusterCacheStatus cacheABC = cacheABCD.copy();
      cacheABC.removeMembers(D);
      List<Address> remainingMembers = asList(B, C, D);

      Map<Address, CacheStatusResponse> statusResponses = mapOf(
            B, availableResponse(B, cacheABC),
            C, availableResponse(C, cacheABC),
            D, degradedResponse(D, cacheABCD));

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(true);
      when(context.calculateConflictHash(any(), any(), any()))
            .thenReturn(cacheABC.readConsistentHash());

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).queueConflictResolution(any(), any());
      verify(context, never()).queueRebalance(any());
   }

   public void testAllDegradedMergeTriggersConflictResolution() {
      TestClusterCacheStatus cacheABCD = start(DIST_INFO, A, B, C, D);
      TestClusterCacheStatus cacheAB = cacheABCD.copy();
      cacheAB.removeMembers(C, D);
      TestClusterCacheStatus cacheCD = cacheABCD.copy();
      cacheCD.removeMembers(A, B);
      List<Address> mergedMembers = asList(A, B, C, D);

      Map<Address, CacheStatusResponse> statusResponses = mapOf(
            A, degradedResponse(A, cacheAB),
            B, degradedResponse(B, cacheAB),
            C, degradedResponse(C, cacheCD),
            D, degradedResponse(D, cacheCD));

      when(context.getExpectedMembers()).thenReturn(mergedMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(true);
      when(context.calculateConflictHash(any(), any(), any()))
            .thenReturn(cacheAB.readConsistentHash());

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).queueConflictResolution(any(), any());
      verify(context, never()).queueRebalance(any());
   }

   public void testIndependentClustersMergeTriggersConflictResolution() {
      TestClusterCacheStatus cacheA = start(DIST_INFO, A);
      TestClusterCacheStatus cacheB = start(DIST_INFO, B);
      List<Address> mergedMembers = asList(A, B);

      Map<Address, CacheStatusResponse> statusResponses = mapOf(
            A, availableResponse(A, cacheA),
            B, availableResponse(B, cacheB));

      when(context.getExpectedMembers()).thenReturn(mergedMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(true);
      when(context.calculateConflictHash(any(), any(), any()))
            .thenReturn(cacheA.readConsistentHash());

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).queueConflictResolution(any(), any());
      verify(context, never()).queueRebalance(any());
   }

   public void testEmptyStatusResponsesSkipsUpdate() {
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, Collections.emptyMap());

      verify(context, never()).updateTopologiesAfterMerge(any(), any(), any());
      verify(context, never()).queueConflictResolution(any(), any());
      verify(context, never()).queueRebalance(any());
   }

   private CacheStatusResponse availableResponse(Address sender, TestClusterCacheStatus cacheStatus) {
      return new CacheStatusResponse(cacheStatus.joinInfo(sender), cacheStatus.topology(),
                                     cacheStatus.stableTopology(), AVAILABLE,
                                     cacheStatus.topology().getActualMembers(), null);
   }

   private CacheStatusResponse degradedResponse(Address sender, TestClusterCacheStatus cacheStatus) {
      return new CacheStatusResponse(cacheStatus.joinInfo(sender), cacheStatus.topology(),
                                     cacheStatus.stableTopology(), DEGRADED_MODE,
                                     cacheStatus.topology().getActualMembers(), null);
   }

   private CacheStatusResponse joinerResponse() {
      return new CacheStatusResponse(DIST_INFO, null, null, AVAILABLE, null, null);
   }
}
