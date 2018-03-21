package org.infinispan.partitionhandling.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.test.TestingUtil.mapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test how PreferAvailabilityStrategy picks the post-merge topology in different scenarios.
 *
 * @author Dan Berindei
 * @since 9.2
 */
@Test(groups = "unit", testName = "partitionhandling.impl.PreferAvailabilityStrategyNoConflictResolutionTest")
public class PreferAvailabilityStrategyNoConflictResolutionTest extends AbstractInfinispanTest {
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

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      persistentUUIDManager = new PersistentUUIDManagerImpl();
      eventLogManager = new EventLogManagerImpl();
      context = mock(AvailabilityStrategyContext.class);

      persistentUUIDManager.addPersistentAddressMapping(A, TestClusterCacheStatus.persistentUUID(A));
      persistentUUIDManager.addPersistentAddressMapping(B, TestClusterCacheStatus.persistentUUID(B));
      persistentUUIDManager.addPersistentAddressMapping(C, TestClusterCacheStatus.persistentUUID(C));

      strategy = new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager,
                                                ClusterTopologyManagerImpl::distLostDataCheck);
   }

   public void testCoordinatorChangeTopologyNotUpdatedAfterLeave() {
      TestClusterCacheStatus cache = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      List<Address> remainingMembers = asList(A, B);
      CacheStatusResponse responseA = availableResponse(A, cache);
      CacheStatusResponse responseB = availableResponse(B, cache);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(false);

      strategy.onPartitionMerge(context, statusResponses);

      // FIXME Either the stable topology id should also be incremented, or even better the current topology id
      // shouldn't change if all members report the same topology
      cache.incrementIds(1, 1);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      verify(context).updateTopologiesAfterMerge(cache.topology(), cache.stableTopology(), null, false);
      verify(context).updateCurrentTopology(remainingMembers);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testCoordinatorChangeTopologyPartiallyUpdatedAfterLeave() {
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      List<Address> remainingMembers = asList(A, B);
      TestClusterCacheStatus cacheStatusB = cacheA.copy();
      cacheStatusB.removeMember(C);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheStatusB);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(false);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.incrementIds(2, 1);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      // FIXME Node B's topology is more recent, so it should be used instead
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, false);
      verify(context).updateCurrentTopology(remainingMembers);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testCoordinatorChangeLeaveDuringRebalanceReadOld() {
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B);
      List<Address> remainingMembers = asList(A, C);
      TestClusterCacheStatus cacheC = cacheA.copy();
      cacheC.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheC.removeMember(B);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(false);
      when(context.getStableTopology()).thenReturn(cacheA.stableTopology());
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.incrementIds(3, 2);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      // FIXME Node C's topology is more recent, so it should be used instead
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, false);
      // FIXME Shouldn't be used at all during merge
      verify(context).getStableTopology();
      verify(context).getCacheName();
      verify(context).updateCurrentTopology(singletonList(A));
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testCoordinatorChangeLeaveDuringRebalanceReadNew() {
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B);
      cacheA.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheA.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      List<Address> remainingMembers = asList(A, C);
      TestClusterCacheStatus cacheC = cacheA.copy();
      cacheC.removeMember(B);
      cacheC.advanceRebalance(CacheTopology.Phase.READ_NEW_WRITE_ALL);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(false);
      when(context.getStableTopology()).thenReturn(cacheA.stableTopology());
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.cancelRebalance();
      cacheA.incrementIds(2, 1);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, false);
      // FIXME Shouldn't be used here
      verify(context).getStableTopology();
      verify(context).getCacheName();
      verify(context).updateCurrentTopology(singletonList(A));
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMergeWithPausedNodeDuringRebalance() {
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      TestClusterCacheStatus cacheC = cacheB.copy();
      cacheC.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(false);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.cancelRebalance();
      cacheA.incrementIds(2, 2);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, false);
      verify(context).updateCurrentTopology(asList(A, B, C));
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   public void testMergeWithPausedNodeAfterRebalance() {
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheB.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      cacheB.finishRebalance();
      cacheB.updateStableTopology();
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      CacheStatusResponse responseC = availableResponse(C, cacheB);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(false);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.cancelRebalance();
      cacheA.incrementIds(3, 2);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      // FIXME This is wrong, should use the topologies of B and C
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, false);
      verify(context).updateCurrentTopology(asList(A, B, C));
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   private CacheStatusResponse availableResponse(Address a, TestClusterCacheStatus cacheStatus) {
      return new CacheStatusResponse(cacheStatus.joinInfo(a), cacheStatus.topology(), cacheStatus.stableTopology(),
                                     AVAILABLE);
   }

}
