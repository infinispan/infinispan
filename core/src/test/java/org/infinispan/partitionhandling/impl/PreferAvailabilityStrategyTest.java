package org.infinispan.partitionhandling.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.partitionhandling.impl.PreferAvailabilityStrategyTest.ConflictResolution.IGNORE;
import static org.infinispan.partitionhandling.impl.PreferAvailabilityStrategyTest.ConflictResolution.RESOLVE;
import static org.infinispan.test.TestingUtil.mapOf;
import static org.infinispan.test.TestingUtil.setOf;
import static org.infinispan.topology.TestClusterCacheStatus.conflictResolutionConsistentHash;
import static org.infinispan.topology.TestClusterCacheStatus.persistentUUID;
import static org.infinispan.topology.TestClusterCacheStatus.start;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.topology.TestClusterCacheStatus;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test how PreferAvailabilityStrategy picks the post-merge topology in different scenarios.
 *
 * @author Dan Berindei
 * @since 9.2
 */
@Test(groups = "unit", testName = "partitionhandling.impl.PreferAvailabilityStrategyNoConflictResolutionTest")
public class PreferAvailabilityStrategyTest extends AbstractInfinispanTest {
   private final ConflictResolution conflicts;

   public enum ConflictResolution {
      RESOLVE, IGNORE;

      boolean resolve() {
         return this == RESOLVE;
      }
   }

   private static final CacheJoinInfo JOIN_INFO =
      new CacheJoinInfo(new DefaultConsistentHashFactory(), MurmurHash3.getInstance(), 8, 2, 1000, false,
                        CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());
   private static final Address A = new TestAddress(1, "A");
   private static final Address B = new TestAddress(2, "B");
   private static final Address C = new TestAddress(3, "C");
   public static final String CACHE_NAME = "test";

   private EventLogManagerImpl eventLogManager;
   private PersistentUUIDManagerImpl persistentUUIDManager;
   private AvailabilityStrategyContext context;
   private PreferAvailabilityStrategy strategy;
   private MockitoSession mockitoSession;

   @DataProvider
   public static Object[][] conflictResolutionProvider() {
      return new Object[][]{{RESOLVE}, {IGNORE}};
   }

   @Factory(dataProvider = "conflictResolutionProvider")
   public PreferAvailabilityStrategyTest(ConflictResolution conflicts) {
      this.conflicts = conflicts;
   }

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession()
                              .strictness(Strictness.STRICT_STUBS)
                              .startMocking();
      persistentUUIDManager = new PersistentUUIDManagerImpl();
      eventLogManager = new EventLogManagerImpl();
      context = mock(AvailabilityStrategyContext.class);

      persistentUUIDManager.addPersistentAddressMapping(A, persistentUUID(A));
      persistentUUIDManager.addPersistentAddressMapping(B, persistentUUID(B));
      persistentUUIDManager.addPersistentAddressMapping(C, persistentUUID(C));

      strategy = new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager,
                                                ClusterTopologyManagerImpl::distLostDataCheck);
   }

   @AfterMethod(alwaysRun = true)
   public void teardown() {
      mockitoSession.finishMocking();
   }

   public void testSinglePartitionOnlyJoiners() {
      // There's no cache topology, so the first cache topology is created with the joiners
      List<Address> joiners = asList(A, B);
      CacheStatusResponse response = new CacheStatusResponse(JOIN_INFO, null, null, AVAILABLE);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, response, B, response);

      when(context.getCacheName()).thenReturn(CACHE_NAME);
      when(context.getExpectedMembers()).thenReturn(joiners);

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).updateCurrentTopology(joiners);
      verify(context).queueRebalance(joiners);
      verifyNoMoreInteractions(context);
   }

   public void testSinglePartitionJoinersAndMissingNode() {
      // B and C both tried to join, but only B got a response from the old coordinator
      List<Address> mergeMembers = asList(B, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A);
      CacheStatusResponse responseB = availableResponse(B, cacheA);
      CacheStatusResponse responseC = new CacheStatusResponse(JOIN_INFO, null, null, AVAILABLE);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(B, responseB, C, responseC);

      when(context.getCacheName()).thenReturn(CACHE_NAME);
      when(context.getExpectedMembers()).thenReturn(mergeMembers);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheA.copy();
      expectedCache.incrementIds();
      verify(context).updateCurrentTopology(mergeMembers);
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testSinglePartitionTopologyNotUpdatedAfterLeave() {
      // A crashed and it's the next coordinator's job to remove it from the cache topology
      List<Address> remainingMembers = asList(B, C);
      TestClusterCacheStatus cacheABC = start(JOIN_INFO, A, B, C);
      CacheStatusResponse responseB = availableResponse(B, cacheABC);
      CacheStatusResponse responseC = availableResponse(C, cacheABC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheABC.copy();
      expectedCache.updateActualMembers(B, C);
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).updateCurrentTopology(remainingMembers);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testSinglePartitionTopologyPartiallyUpdatedAfterLeave() {
      // A crashed, but only C has the updated cache topology
      List<Address> remainingMembers = asList(B, C);
      TestClusterCacheStatus cacheAB = start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheC = cacheAB.copy();
      cacheC.removeMembers(A);
      CacheStatusResponse responseB = availableResponse(B, cacheAB);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheC.copy();
      expectedCache.incrementIds();
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testSinglePartitionLeaveDuringRebalancePhaseReadOld() {
      // C joins and rebalance starts, but A crashes and B doesn't receive either rebalance start or leave topology
      // Leave topology updates are fire-and-forget, so it's possible for A to miss both the leave topology
      // and the phase change and still have a single partition
      // However, PreferAvailabilityStrategy will not recognize it as a single partition
      List<Address> remainingMembers = asList(B, C);
      TestClusterCacheStatus cacheAB = start(JOIN_INFO, A, B);
      TestClusterCacheStatus cacheC = cacheAB.copy();
      cacheC.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheC.removeMembers(A);
      CacheStatusResponse responseB = availableResponse(B, cacheAB);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheC.copy();
      expectedCache.cancelRebalance();
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testSinglePartitionLeaveDuringRebalancePhaseReadNew() {
      // C joins and rebalance starts, but A crashes and B doesn't receive 2 topology updates (rebalance phase + leave)
      // Leave topology updates are fire-and-forget, so it's possible for B to miss both the leave topology
      // and the phase change and still have a single partition
      List<Address> mergeMembers = asList(B, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A, B);
      cacheA.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheA.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      TestClusterCacheStatus cacheC = cacheA.copy();
      cacheC.removeMembers(A);
      cacheC.advanceRebalance(CacheTopology.Phase.READ_NEW_WRITE_ALL);
      CacheStatusResponse responseB = availableResponse(B, cacheA);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheC.copy();
      expectedCache.cancelRebalance();
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testSinglePartitionOneNodeSplits() {
      // C starts a partition by itself
      TestClusterCacheStatus cacheABC = start(JOIN_INFO, A, B, C);
      List<Address> remainingMembers = singletonList(C);
      CacheStatusResponse responseC = availableResponse(C, cacheABC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheABC.copy();
      expectedCache.updateActualMembers(C);
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).updateCurrentTopology(remainingMembers);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMerge1Paused2Rebalancing() {
      // A was paused and keeps the stable topology, B and C are rebalancing
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.removeMembers(A);
      cacheB.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, B, C);
      TestClusterCacheStatus cacheC = cacheB.copy();
      cacheC.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheC.copy();
      expectedCache.cancelRebalance();
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMerge1Paused2StableAfterRebalance() {
      // A was paused and keeps the stable topology, B and C finished rebalancing and have a new stable topology
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheBC = cacheA.copy();
      cacheBC.removeMembers(A);
      cacheBC.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, B, C);
      cacheBC.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      cacheBC.finishRebalance();
      cacheBC.updateStableTopology();
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheBC);
      CacheStatusResponse responseC = availableResponse(C, cacheBC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheBC.copy();
      expectedCache.incrementIds();
      expectedCache.incrementIdsIfNeeded(cacheA);
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMerge1Paused2StableNoRebalance() {
      // A was paused and keeps the stable topology, B has a new stable topology (no rebalance needed)
      // No conflict resolution needed, because B has all the data
      List<Address> mergeMembers = asList(A, B);
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.removeMembers(A);
      cacheB.updateStableTopology();
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheB.copy();
      expectedCache.incrementIds();
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null, false);
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMerge1Paused2StableAfterLosingAnotherNode() {
      // A was paused and keeps the stable topology
      // B and C finished rebalancing, then B was paused
      // Now A has resumed and merges with C
      // Conflict resolution is needed because A might have changed some keys by talking only to B
      List<Address> mergeMembers = asList(A, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.removeMembers(A);
      cacheB.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, B, C);
      cacheB.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      cacheB.finishRebalance();
      cacheB.updateStableTopology();
      TestClusterCacheStatus cacheC = cacheB.copy();
      cacheC.removeMembers(B);
      cacheC.updateStableTopology();
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.getCacheName()).thenReturn(CACHE_NAME);
      when(context.resolveConflictsOnMerge()).thenReturn(conflicts.resolve());
      if (conflicts.resolve()) {
         when(context.calculateConflictHash(cacheA.readConsistentHash(),
                                            setOf(cacheA.readConsistentHash(), cacheC.readConsistentHash())))
            .thenReturn(conflictResolutionConsistentHash(cacheA, cacheC));
      }

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheA.copy();
      expectedCache.updateActualMembers(mergeMembers);
      if (conflicts.resolve()) {
         expectedCache.startConflictResolution(conflictResolutionConsistentHash(cacheA, cacheC), A, C);
      }
      expectedCache.incrementIdsIfNeeded(cacheC);
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null,
                                                 conflicts.resolve());
      if (!conflicts.resolve()) {
         verify(context).updateCurrentTopology(mergeMembers);
      }
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMerge1HigherTopologyId2MoreNodesSameStableTopology() {
      // Partition A has a higher topology id, but BCD should win because it is larger
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheBC = cacheA.copy();
      cacheA.removeMembers(B);
      cacheA.removeMembers(C);
      cacheBC.removeMembers(A);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheBC);
      CacheStatusResponse responseC = availableResponse(C, cacheBC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);
      assertTrue(cacheA.topology().getTopologyId() > cacheBC.topology().getTopologyId());
      assertSame(cacheA.stableTopology(), cacheBC.stableTopology());

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(conflicts.resolve());
      when(context.getCacheName()).thenReturn(CACHE_NAME);
      if (conflicts.resolve()) {
         when(context.calculateConflictHash(cacheBC.readConsistentHash(),
                                            setOf(cacheA.readConsistentHash(), cacheBC.readConsistentHash())))
            .thenReturn(conflictResolutionConsistentHash(cacheA, cacheBC));
      }

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheBC.copy();
      if (conflicts.resolve()) {
         expectedCache.startConflictResolution(conflictResolutionConsistentHash(cacheA, cacheBC), A, B, C);
      } else {
         expectedCache.incrementIds();
      }
      expectedCache.incrementIdsIfNeeded(cacheA);
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null,
                                                 conflicts.resolve());
      if (!conflicts.resolve()) {
         verify(context).updateCurrentTopology(expectedCache.topology().getMembers());
      }
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMerge1HigherTopologyId2MoreNodesIndependentStableTopology() {
      // Partition A has a higher topology id, but BC should win because it is larger
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = start(JOIN_INFO, A);
      cacheA.incrementIds();
      TestClusterCacheStatus cacheBC = start(JOIN_INFO, B, C);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheBC);
      CacheStatusResponse responseC = availableResponse(C, cacheBC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);
      assertTrue(cacheA.topology().getTopologyId() > cacheBC.topology().getTopologyId());

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(conflicts.resolve());
      when(context.getCacheName()).thenReturn(CACHE_NAME);
      if (conflicts.resolve()) {
         when(context.calculateConflictHash(cacheBC.readConsistentHash(),
                                            setOf(cacheA.readConsistentHash(), cacheBC.readConsistentHash())))
            .thenReturn(conflictResolutionConsistentHash(cacheA, cacheBC));
      }

      strategy.onPartitionMerge(context, statusResponses);

      TestClusterCacheStatus expectedCache = cacheBC.copy();
      if (conflicts.resolve()) {
         expectedCache.startConflictResolution(conflictResolutionConsistentHash(cacheA, cacheBC), A, B, C);
      } else {
         expectedCache.incrementIds();
      }
      expectedCache.incrementIdsIfNeeded(cacheA);
      verify(context).updateTopologiesAfterMerge(expectedCache.topology(), expectedCache.stableTopology(), null,
                                                 conflicts.resolve());
      if (!conflicts.resolve()) {
         verify(context).updateCurrentTopology(expectedCache.topology().getMembers());
      }
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   private CacheStatusResponse availableResponse(Address a, TestClusterCacheStatus cacheStatus) {
      return new CacheStatusResponse(cacheStatus.joinInfo(a), cacheStatus.topology(), cacheStatus.stableTopology(),
                                     AVAILABLE);
   }

   @Override
   protected String parameters() {
      return "[" + conflicts + "]";
   }
}
